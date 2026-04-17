import { useMemo } from 'react'
import { useDashboard } from './hooks/useDashboard'
import UserCard from './components/UserCard'
import WorkerNode from './components/WorkerNode'
import TranscriptionLine from './components/TranscriptionLine'
import ConnectionLines from './components/ConnectionLines'
import SpeakiSprite from './components/SpeakiSprite'
import VoteBanner from './components/VoteBanner'
import type { WorkerDisplayState } from './types'
import './app.css'

declare const __GIT_HASH__: string

const WS_URL = `${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`

export default function App() {
  const state = useDashboard(WS_URL)

  const workers = useMemo<WorkerDisplayState[]>(() => {
    const now = Date.now()
    return Array.from({ length: state.worker_count }, (_, i) => {
      const recent = state.active_routes
        .slice()
        .reverse()
        .find((r) => r.worker_idx === i && now - r.at < 1600)
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
        <div className="content-card">
          <ConnectionLines routes={state.active_routes} />

          <div className="card-header">
            <span className="card-title">
              speakispeaki{' '}
              <span className="card-hash">v{__GIT_HASH__}</span>
            </span>
            <span className="card-channel">
              <span className={`status-dot${state.connected ? ' status-dot--ok' : ' status-dot--off'}`} />
              #{state.channel_name}
              {state.max_workers > 0 && (
                <span className="worker-pill">
                  {state.worker_count}/{state.max_workers}w
                </span>
              )}
            </span>
          </div>

          <div className="main-grid">
            <div className="col-users">
              {state.users.map((u) => (
                <UserCard key={u.user_id} user={u} />
              ))}
            </div>

            <div className="col-workers">
              {workers.map((w) => (
                <WorkerNode key={w.idx} worker={w} />
              ))}
            </div>

            <div className="col-tx">
              {state.users.map((u) => (
                <TranscriptionLine key={u.user_id} user={u} />
              ))}
            </div>
          </div>
        </div>
      )}

      <div className="sprite-anchor">
        <SpeakiSprite triggerAt={state.trigger_at} />
      </div>

      {state.vote_info && <VoteBanner info={state.vote_info} />}
    </div>
  )
}
