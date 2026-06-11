import { HttpError } from './api';

// Shared async-load state for detail views: a `loading` flag, an `errorStatus`
// (the HTTP status, or -1 for a network/transport error), and a request-token
// guard so out-of-order responses can't clobber newer data. A fetch is only
// applied if it is still the most recent one issued from this loader.
//
// `run(fetcher)` returns the resolved value on success, or `undefined` when the
// call errored OR was superseded by a newer run — callers apply the value only
// when it is defined. Payloads here are always objects, so `undefined` is an
// unambiguous "don't apply" signal.
export function createLoader() {
  let loading = $state(true);
  let errorStatus = $state<number | null>(null);
  let seq = 0;

  async function run<T>(fetcher: () => Promise<T>): Promise<T | undefined> {
    const token = ++seq;
    loading = true;
    errorStatus = null;
    try {
      const value = await fetcher();
      if (token !== seq) return undefined; // a newer run superseded this one
      return value;
    } catch (e) {
      if (token !== seq) return undefined;
      errorStatus = e instanceof HttpError ? e.status : -1;
      return undefined;
    } finally {
      if (token === seq) loading = false;
    }
  }

  return {
    run,
    get loading() {
      return loading;
    },
    get errorStatus() {
      return errorStatus;
    }
  };
}
