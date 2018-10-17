import {Action, ActionTypes} from 'src/tasks/actions/v2'
import {Task} from 'src/types/v2/tasks'

export interface State {
  newScript: string
  tasks: Task[]
}

const defaultState: State = {
  newScript: '',
  tasks: [],
}

export default (state: State = defaultState, action: Action): State => {
  switch (action.type) {
    case ActionTypes.SetNewScript:
      return {...state, newScript: action.payload.script}
    case ActionTypes.SetTasks:
      return {...state, tasks: action.payload.tasks}
    default:
      return state
  }
}
