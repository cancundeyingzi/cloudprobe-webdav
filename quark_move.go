package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"time"
)

func (d *quarkDAVDrive) renameObject(ctx context.Context, it davEntry, name string) error {
	if err := ctx.Err(); err != nil {
		return &davNotExecuted{err}
	}
	if it.Parent == "" || it.ID == "" {
		return &davNotExecuted{errors.New("缺少对象 ID 或原父目录，无法核验改名")}
	}
	_, err := d.mutate(ctx, "重命名", map[string]any{"fid": it.ID, "file_name": name}, map[string]any{"fid": it.ID, "file_name": name, "conflict_mode": "FAIL"}, "/file/rename", "/open/v1/file/rename", func(check context.Context, _ map[string]any) error {
		return d.verifyMutation(check, it.Parent, it.ID, name, false)
	})
	if err == nil {
		err = d.verifyMutation(ctx, it.Parent, it.ID, name, false)
	}
	return err
}

func (d *quarkDAVDrive) moveObject(ctx context.Context, it davEntry, parent string) error {
	if err := ctx.Err(); err != nil {
		return &davNotExecuted{err}
	}
	_, err := d.mutate(ctx, "移动", map[string]any{"action_type": 1, "filelist": []string{it.ID}, "exclude_fids": []string{}, "to_pdir_fid": parent}, map[string]any{"action_type": 1, "fid_list": []string{it.ID}, "to_pdir_fid": parent}, "/file/move", "/open/v1/file/move", func(check context.Context, _ map[string]any) error {
		return d.verifyMutation(check, parent, it.ID, it.Name, false)
	})
	if err == nil {
		err = d.verifyMutation(ctx, parent, it.ID, it.Name, false)
	}
	return err
}

// Cross-directory rename uses a name absent from both directories. Renaming to
// the final name in the source, or moving under the old name, can each collide
// with an unrelated object even when the requested destination is available.
func (d *quarkDAVDrive) moveRenamed(ctx context.Context, original davEntry, parent, name string) error {
	if original.Parent == "" || original.ID == "" {
		return &davNotExecuted{errors.New("缺少对象 ID 或原父目录，无法核验跨目录改名")}
	}
	source, err := d.List(directoryLogContext(ctx), original.Parent)
	if err != nil {
		return &davNotExecuted{err}
	}
	current, err := uniqueNamedEntry(source, original.Name)
	if err != nil || current == nil || !sameCloudObject(*current, original) || current.Directory != original.Directory {
		return &davNotExecuted{errors.New("原对象已变化，未开始跨目录改名")}
	}
	target, err := d.List(directoryLogContext(ctx), parent)
	if err != nil {
		return &davNotExecuted{err}
	}
	if found, err := uniqueNamedEntry(target, name); err != nil {
		return &davNotExecuted{err}
	} else if found != nil {
		return &davNotExecuted{fmt.Errorf("目标名称已存在，未开始移动：%w", os.ErrExist)}
	}
	staged := original
	for {
		staged.Name = ".cloudprobe-move-" + randomKey()[:24]
		a, ea := uniqueNamedEntry(source, staged.Name)
		b, eb := uniqueNamedEntry(target, staged.Name)
		if ea == nil && eb == nil && a == nil && b == nil {
			break
		}
	}
	if err := d.renameObject(ctx, original, staged.Name); err != nil {
		return fmt.Errorf("跨目录改名准备（对象 %s，临时名 %s）：%w", original.ID, staged.Name, err)
	}
	if err := d.moveObject(ctx, staged, parent); err != nil {
		return d.finishFailedMove(ctx, original, staged, err)
	}
	staged.Parent = parent
	if err := d.renameObject(ctx, staged, name); err != nil {
		return d.finishFailedMove(ctx, original, staged, err)
	}
	return nil
}

func (d *quarkDAVDrive) finishFailedMove(ctx context.Context, original, staged davEntry, cause error) error {
	var notExecuted *davNotExecuted
	var uncertain *davMutationError
	// Absence in a directory cannot prove that an asynchronous or lost request
	// will never complete. Roll back only an explicitly undispatched/rejected
	// step, then revalidate the object and every restoration destination.
	if errors.As(cause, &notExecuted) && !errors.As(cause, &uncertain) {
		check, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := d.restoreStagedMove(check, original, staged); err == nil {
			return &davMutationError{Phase: "跨目录改名（已核验恢复原位置和名称）", Partial: true, Err: cause}
		} else {
			cause = errors.Join(cause, fmt.Errorf("恢复未完成：%w", err))
		}
	}
	return &davMutationError{Phase: "跨目录改名", Partial: true, Err: fmt.Errorf("对象 %s，最后核验位置 %s/%s；请核对实际位置后重试：%w", staged.ID, staged.Parent, staged.Name, cause)}
}

func (d *quarkDAVDrive) restoreStagedMove(ctx context.Context, original, staged davEntry) error {
	entries, err := d.List(directoryLogContext(ctx), staged.Parent)
	if err != nil {
		return err
	}
	found, err := uniqueNamedEntry(entries, staged.Name)
	if err != nil || found == nil || !sameCloudObject(*found, staged) {
		return errors.New("暂存对象身份或位置已变化")
	}
	if staged.Parent != original.Parent {
		entries, err = d.List(directoryLogContext(ctx), original.Parent)
		if err != nil {
			return err
		}
		if occupied, err := uniqueNamedEntry(entries, staged.Name); err != nil || occupied != nil {
			return errors.New("原目录的暂存名称已被占用")
		}
	}
	if occupied, err := uniqueNamedEntry(entries, original.Name); err != nil || occupied != nil {
		return errors.New("原名称已被其他对象占用")
	}
	if staged.Parent != original.Parent {
		if err := d.moveObject(ctx, staged, original.Parent); err != nil {
			return err
		}
		staged.Parent = original.Parent
	}
	return d.renameObject(ctx, staged, original.Name)
}
