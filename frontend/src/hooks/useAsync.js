import { useState, useCallback, useRef } from 'react'

/**
 * Generic hook that wraps an async function with loading / error / data state.
 *
 * Usage:
 *   const { data, loading, error, execute } = useAsync(datasetsAPI.getAll)
 *   useEffect(() => { execute() }, [execute])
 */
export function useAsync(asyncFn) {
  const [state, setState] = useState({ data: null, loading: false, error: null })
  const mountedRef = useRef(true)

  const execute = useCallback(
    async (...args) => {
      setState((s) => ({ ...s, loading: true, error: null }))
      try {
        const data = await asyncFn(...args)
        if (mountedRef.current) setState({ data, loading: false, error: null })
        return data
      } catch (err) {
        if (mountedRef.current)
          setState((s) => ({ ...s, loading: false, error: err.message }))
        throw err
      }
    },
    [asyncFn]
  )

  return { ...state, execute }
}
