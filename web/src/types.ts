export interface MemberInfo {
  user_id: string
  user_label: string
  avatar_url: string | null
}

export interface TranscriptionEntry {
  id: number
  text: string
  wakeword: string | null
  at: number
}

export interface UserState extends MemberInfo {
  amplitudeHistory: number[]
  transcription: TranscriptionEntry[]
}

export interface WorkerDisplayState {
  idx: number
  active_user_id: string | null
}

export interface ActiveRoute {
  id: number
  user_id: string
  worker_idx: number
  at: number
}

export interface VoteInfo {
  voter_label: string
  votes: number
  needed: number
}

export interface AppState {
  connected: boolean
  reconnecting: boolean
  channel_name: string
  guild_name: string
  users: UserState[]
  worker_count: number
  max_workers: number
  active_routes: ActiveRoute[]
  vote_info: VoteInfo | null
  session_closed: string | null
  trigger_at: number
}

export type DashboardEvent =
  | {
      type: 'session_state'
      guild_name: string
      channel_name: string
      worker_count: number
      max_workers: number
      members: MemberInfo[]
      transcription_history: Record<string, Array<{ text: string; wakeword: string | null }>>
    }
  | {
      type: 'live_audio'
      user_id: string
      user_label: string
      avatar_url: string | null
      amplitude: number
    }
  | { type: 'worker_routing'; user_id: string; worker_idx: number }
  | {
      type: 'trigger'
      user_id: string
      user_label: string
      text: string
      worker_idx: number
    }
  | {
      type: 'transcription'
      user_id: string
      user_label: string
      text: string
      wakeword: string | null
    }
  | { type: 'member_join'; user_id: string; user_label: string; avatar_url: string | null }
  | { type: 'member_leave'; user_id: string }
  | { type: 'worker_pool_resize'; count: number; max: number }
  | { type: 'vote_update'; voter_label: string; votes: number; needed: number }
  | { type: 'session_close'; reason: string }
