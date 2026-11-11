package main

import (
	"context"
	"time"
)

func (g *guiApp) newWopanAccountDrive(cl *Client, a guiAccount, root string, save func(string, string) error) *wopanDAVDrive {
	d := newWopanDAVDrive(cl, a.credentials, root, save)
	d.accountCredentials = func() map[string]any {
		g.mu.Lock()
		defer g.mu.Unlock()
		return cloneGUIAccount(g.accounts[a.Key]).credentials
	}
	d.webAccess = wopanWebToken(a.credentials)
	return d
}

// Caller holds directMu. Existing upload sessions keep their captured token;
// subsequent operations can use newly authorized channels without stopping DAV.
func (d *wopanDAVDrive) syncAccountCredential() bool {
	if d.accountCredentials == nil {
		return false
	}
	c := d.accountCredentials()
	web := wopanWebToken(c)
	if web == d.webAccess {
		return false
	}
	d.webAccess = web
	d.credentialNeedsProbe = true
	if web != "" {
		d.w.SetAccessToken(web)
		d.w.SetRefreshToken("")
		d.setWebMode(true)
	} else {
		d.w.SetAccessToken(d.client.cfg.Wopan.AccessToken)
		d.w.SetRefreshToken(d.serverRefresh)
		d.setWebMode(false)
	}
	return true
}
func (d *wopanDAVDrive) setWebMode(web bool) {
	d.writeStateMu.Lock()
	d.usingWeb = web
	d.writeStateMu.Unlock()
}
func (d *wopanDAVDrive) otherAuthorization() bool {
	if d.usingWeb {
		if d.client.cfg.Wopan.AccessToken == "" {
			return false
		}
		d.w.SetAccessToken(d.client.cfg.Wopan.AccessToken)
		d.w.SetRefreshToken(d.serverRefresh)
		d.setWebMode(false)
		return true
	}
	if d.webAccess == "" {
		return false
	}
	d.w.SetAccessToken(d.webAccess)
	d.w.SetRefreshToken("")
	d.setWebMode(true)
	return true
}
func (d *wopanDAVDrive) refreshAccountWrite(ctx context.Context) error {
	if err := d.directMu.LockContext(ctx); err != nil {
		return err
	}
	d.syncAccountCredential()
	changed := d.credentialNeedsProbe
	d.directMu.Unlock()
	if changed {
		probe, cancel := context.WithTimeout(ctx, 25*time.Second)
		defer cancel()
		return d.probe(probe)
	}
	return d.ready()
}
