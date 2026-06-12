<script lang="ts">
  import { page } from '$app/stores';
  import { LOGIN_ACTION, OIDC_LOGIN_URL, fetchAuthModes, type AuthModes } from '$lib/api';
  import { LOCK_BRAND } from '$lib/icons';

  // The proxy's POST ${base}/api/login validates the form, sets the session
  // cookie, and 303-redirects (to ${base}/ on success, back to
  // ${base}/login?error=1 on failure). We render the form and let the browser
  // submit it natively, so the auth flow is unchanged. SSO is a plain link to
  // ${base}/api/oidc/login, which redirects to the IdP.
  const action = LOGIN_ACTION;

  let modes = $state<AuthModes>({ password: true, oidc: false, oidc_label: 'Sign in with SSO' });
  $effect(() => {
    fetchAuthModes().then((m) => (modes = m));
  });

  const errorText: Record<string, string> = {
    '1': 'Invalid username or password.',
    sso: 'Single sign-on failed. Please try again.'
  };
  let error = $derived($page.url.searchParams.get('error'));
  let errorMessage = $derived(
    error ? (errorText[error] ?? 'Sign in failed. Please try again.') : ''
  );
</script>

<svelte:head>
  <title>Sign in · S3 Encryption Proxy</title>
</svelte:head>

<main class="login-page">
  <div class="login-card">
    <div class="login-head">
      <span class="brand-mark" aria-hidden="true">{@html LOCK_BRAND}</span>
      <div>
        <div class="login-title">S3 Encryption Proxy</div>
        <div class="login-subtitle">Sign in to the dashboard</div>
      </div>
    </div>

    {#if errorMessage}
      <div class="login-error">{errorMessage}</div>
    {/if}

    {#if modes.oidc}
      <a class="btn-dark login-submit login-sso" href={OIDC_LOGIN_URL}>{modes.oidc_label}</a>
    {/if}

    {#if modes.password && modes.oidc}
      <div class="login-divider"><span>or</span></div>
    {/if}

    {#if modes.password}
      <form method="post" action={action} autocomplete="on">
        <div class="field">
          <label for="u">Username</label>
          <input id="u" name="username" type="text" autocomplete="username" autofocus required />
        </div>
        <div class="field">
          <label for="p">Password</label>
          <input id="p" name="password" type="password" autocomplete="current-password" required />
        </div>

        <button type="submit" class="btn-dark login-submit">Sign in</button>
      </form>
    {/if}
  </div>
</main>

<style>
  .login-sso {
    display: block;
    text-align: center;
    text-decoration: none;
  }
  .login-divider {
    display: flex;
    align-items: center;
    gap: 12px;
    margin: 18px 0;
    color: var(--text-muted);
    font-size: 12px;
  }
  .login-divider::before,
  .login-divider::after {
    content: '';
    flex: 1;
    height: 1px;
    background: var(--border);
  }
</style>
