import { useMemo, useState, useEffect, useRef } from 'react'
import { useDashboard } from './hooks/useDashboard'
import UserCard from './components/UserCard'
import WorkerNode from './components/WorkerNode'
import TranscriptionLine from './components/TranscriptionLine'
import ConnectionLines from './components/ConnectionLines'
import SpeakiSprite from './components/SpeakiSprite'
import VoteBanner from './components/VoteBanner'
import type { WorkerDisplayState, UserState } from './types'
import './app.css'

declare const __COMMIT_COUNT__: string

const WS_URL = `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`

export default function App() {
  const state = useDashboard(WS_URL)

  // Lazy user ordering: positions are stable; re-sort only every 3 s so active users
  // rise to the top without constant shuffling during lively conversation.
  const usersRef = useRef(state.users)
  usersRef.current = state.users

  const [displayOrder, setDisplayOrder] = useState<string[]>([])

  const userIdKey = state.users.map(u => u.user_id).join(',')
  useEffect(() => {
    setDisplayOrder(prev => {
      const currentIds = new Set(state.users.map(u => u.user_id))
      const filtered = prev.filter(id => currentIds.has(id))
      const existing = new Set(filtered)
      const newIds = state.users.filter(u => !existing.has(u.user_id)).map(u => u.user_id)
      return [...filtered, ...newIds]
    })
  // userIdKey is the stable dep; state.users intentionally omitted to avoid running on every amplitude update
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [userIdKey])

  useEffect(() => {
    const id = setInterval(() => {
      setDisplayOrder(
        usersRef.current
          .slice()
          .sort((a, b) => b.last_active_at - a.last_active_at)
          .map(u => u.user_id),
      )
    }, 3000)
    return () => clearInterval(id)
  }, [])

  const sortedUsers = useMemo(
    () => displayOrder
      .map(id => state.users.find(u => u.user_id === id))
      .filter((u): u is UserState => u !== undefined),
    [displayOrder, state.users],
  )

  const workers = useMemo<WorkerDisplayState[]>(() => {
    const now = Date.now()
    return Array.from({ length: state.worker_count }, (_, i) => {
      const recent = state.active_routes
        .slice()
        .reverse()
        .find((r) => r.worker_idx === i && now - r.at < 3500)
      return { idx: i, active_user_id: recent?.user_id ?? null }
    })
  }, [state.worker_count, state.active_routes])

  const isIdle = !state.channel_name && !state.session_closed
  const isClosed = !!state.session_closed

  return (
    <div className="app">
      <div className="app-bg" />

      {isClosed ? (
        <div className="overlay">
          <SpeakiSprite triggerAt={0} />
          <p className="overlay-text">
            jo... joayo... speaki went home~
            <br />
            <small>({state.session_closed})</small>
            <br />
            <small>bring her back by typing <code>speaki</code>~!</small>
          </p>
        </div>
      ) : isIdle ? (
        <div className="overlay">
          <SpeakiSprite triggerAt={0} />
          <p className="overlay-text">
            speaki is waiting... type{' '}
            <code>speaki</code> to summon~!
          </p>
        </div>
      ) : (
        <>
          <ConnectionLines routes={state.active_routes} />

          <div className="app-titlebar">
            <span className="app-title">
              speakispeaki <span className="app-title-n">v{__COMMIT_COUNT__}</span>
            </span>
            <span className="app-channel">
              <span className={`status-dot${state.connected ? ' status-dot--ok' : ' status-dot--off'}`} />
              #{state.channel_name}
              <span
                className={`bot-status-pill bot-status-pill--${state.bot_status}`}
                title={state.bot_status_detail || undefined}
              >
                {state.bot_status}
              </span>
              {state.max_workers > 0 && (
                <span className="worker-pill">
                  {state.worker_count}/{state.max_workers}w
                </span>
              )}
            </span>
          </div>

          <div className="content-card">
            <div className="main-grid">
              <div className="col-users">
                {sortedUsers.map((u) => (
                  <UserCard key={u.user_id} user={u} />
                ))}
              </div>

              <div className="col-workers">
                {workers.map((w) => (
                  <WorkerNode key={w.idx} worker={w} />
                ))}
              </div>

              <div className="col-tx">
                {sortedUsers.map((u) => (
                  <TranscriptionLine key={u.user_id} user={u} />
                ))}
              </div>
            </div>
          </div>
        </>
      )}

      <div className="sprite-anchor">
        <SpeakiSprite triggerAt={state.trigger_at} />
      </div>

      {state.vote_info && <VoteBanner info={state.vote_info} />}
    </div>
  )
}
