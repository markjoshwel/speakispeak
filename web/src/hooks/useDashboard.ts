import { useEffect, useReducer, useRef, useCallback } from 'react'
import type { AppState, DashboardEvent, UserState, ActiveRoute } from '../types'

const AMPLITUDE_MAX = 60
const TRANSCRIPTION_MAX = 15
const ROUTE_TTL_MS = 2200

const INITIAL_STATE: AppState = {
  connected: false,
  reconnecting: false,
  channel_name: '',
  guild_name: '',
  users: [],
  worker_count: 0,
  max_workers: 0,
  active_routes: [],
  vote_info: null,
  session_closed: null,
  trigger_at: 0,
  bot_status: 'loading',
  bot_status_detail: 'starting',
}

let _routeId = 0
let _txId = 0

type Action =
  | { type: 'ws_open' }
  | { type: 'ws_close' }
  | { type: 'event'; ev: DashboardEvent }
  | { type: 'expire_routes'; now: number }

function upsertUser(users: UserState[], patch: Partial<UserState> & { user_id: string }): UserState[] {
  const idx = users.findIndex((u) => u.user_id === patch.user_id)
  if (idx === -1) {
    return [
      ...users,
      {
        user_label: patch.user_id,
        avatar_url: null,
        amplitudeHistory: [],
        transcription: [],
        last_active_at: 0,
        ...patch,
      },
    ]
  }
  const next = [...users]
  next[idx] = { ...next[idx], ...patch }
  return next
}

function reducer(state: AppState, action: Action): AppState {
  switch (action.type) {
    case 'ws_open':
      return { ...state, connected: true, reconnecting: false }

    case 'ws_close':
      return { ...state, connected: false, reconnecting: true }

    case 'expire_routes':
      return {
        ...state,
        active_routes: state.active_routes.filter(
          (r) => action.now - r.at < ROUTE_TTL_MS,
        ),
      }

    case 'event': {
      const ev = action.ev
      switch (ev.type) {
        case 'bot_status':
          return { ...state, bot_status: ev.status, bot_status_detail: ev.detail }

        case 'session_state':
          return {
            ...state,
            channel_name: ev.channel_name,
            guild_name: ev.guild_name,
            worker_count: ev.worker_count,
            max_workers: ev.max_workers,
            session_closed: null,
            bot_status: ev.bot_status ?? 'listening',
            bot_status_detail: ev.bot_status_detail ?? '',
            users: ev.members.map((m) => ({
              ...m,
              amplitudeHistory: [],
              transcription: (ev.transcription_history[m.user_id] ?? []).map((h) => ({
                id: ++_txId,
                text: h.text,
                wakeword: h.wakeword,
                at: Date.now(),
              })),
              last_active_at: (ev.transcription_history[m.user_id]?.length ?? 0) > 0 ? Date.now() : 0,
            })),
          }

        case 'live_audio': {
          const existing = state.users.find((u) => u.user_id === ev.user_id)?.amplitudeHistory ?? []
          const users = upsertUser(state.users, {
            user_id: ev.user_id,
            user_label: ev.user_label,
            avatar_url: ev.avatar_url,
            amplitudeHistory: [ev.amplitude, ...existing.slice(0, AMPLITUDE_MAX - 1)],
            last_active_at: Date.now(),
          })
          return { ...state, users }
        }

        case 'worker_routing': {
          const route: ActiveRoute = {
            id: ++_routeId,
            user_id: ev.user_id,
            worker_idx: ev.worker_idx,
            at: Date.now(),
          }
          return {
            ...state,
            active_routes: [...state.active_routes.slice(-30), route],
          }
        }

        case 'transcription': {
          const entry = { id: ++_txId, text: ev.text, wakeword: ev.wakeword, at: Date.now() }
          const existing = state.users.find((u) => u.user_id === ev.user_id)?.transcription ?? []
          const users = upsertUser(state.users, {
            user_id: ev.user_id,
            user_label: ev.user_label,
            transcription: [...existing.slice(-(TRANSCRIPTION_MAX - 1)), entry],
            last_active_at: Date.now(),
          })
          return { ...state, users, trigger_at: ev.wakeword ? Date.now() : state.trigger_at }
        }

        case 'trigger': {
          const entry = { id: ++_txId, text: ev.text, wakeword: ev.text, at: Date.now() }
          const existing = state.users.find((u) => u.user_id === ev.user_id)?.transcription ?? []
          const users = upsertUser(state.users, {
            user_id: ev.user_id,
            user_label: ev.user_label,
            transcription: [...existing.slice(-(TRANSCRIPTION_MAX - 1)), entry],
            last_active_at: Date.now(),
          })
          return { ...state, users, trigger_at: Date.now() }
        }

        case 'member_join':
          if (state.users.find((u) => u.user_id === ev.user_id)) return state
          return {
            ...state,
            users: [
              ...state.users,
              {
                user_id: ev.user_id,
                user_label: ev.user_label,
                avatar_url: ev.avatar_url,
                amplitudeHistory: [],
                transcription: [],
                last_active_at: 0,
              },
            ],
          }

        case 'member_leave':
          return {
            ...state,
            users: state.users.filter((u) => u.user_id !== ev.user_id),
          }

        case 'worker_pool_resize':
          return { ...state, worker_count: ev.count, max_workers: ev.max }

        case 'vote_update':
          return {
            ...state,
            vote_info: {
              voter_label: ev.voter_label,
              votes: ev.votes,
              needed: ev.needed,
            },
          }

        case 'session_close':
          return {
            ...state,
            session_closed: ev.reason,
            users: [],
            active_routes: [],
            vote_info: null,
            channel_name: '',
          }

        default:
          return state
      }
    }

    default:
      return state
  }
}

export function useDashboard(wsUrl: string): AppState {
  const [state, dispatch] = useReducer(reducer, INITIAL_STATE)
  const wsRef = useRef<WebSocket | null>(null)

  const connect = useCallback(() => {
    try {
      const ws = new WebSocket(wsUrl)
      wsRef.current = ws

      ws.onopen = () => dispatch({ type: 'ws_open' })
      ws.onclose = () => {
        dispatch({ type: 'ws_close' })
        setTimeout(connect, 2500)
      }
      ws.onerror = () => ws.close()
      ws.onmessage = (e) => {
        try {
          const ev = JSON.parse(e.data as string) as DashboardEvent
          dispatch({ type: 'event', ev })
        } catch {
          // ignore bad frames
        }
      }
    } catch {
      setTimeout(connect, 2500)
    }
  }, [wsUrl])

  useEffect(() => {
    connect()
    const interval = setInterval(
      () => dispatch({ type: 'expire_routes', now: Date.now() }),
      120,
    )
    return () => {
      clearInterval(interval)
      wsRef.current?.close()
    }
  }, [connect])

  return state
}
