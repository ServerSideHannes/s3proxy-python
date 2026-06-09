<script lang="ts">
  import { page } from '$app/stores';
  import { LOGIN_ACTION } from '$lib/api';
  import { LOCK_BRAND } from '$lib/icons';

  // The proxy's POST ${base}/api/login validates the form, sets the session
  // cookie, and 303-redirects (to ${base}/ on success, back to
  // ${base}/login?error=1 on failure). We render the form and let the browser
  // submit it natively, so the auth flow is unchanged.
  const action = LOGIN_ACTION;

  const errorText: Record<string, string> = {
    '1': 'Invalid username or password.'
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
  <form class="login-card" method="post" action={action} autocomplete="on">
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
</main>
