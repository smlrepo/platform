import React, {Component} from 'react'
import {connect} from 'react-redux'
import {withRouter, InjectedRouter} from 'react-router'

import {searchToFilters} from 'src/logs/utils/search'
import {notify as notifyAction} from 'src/shared/actions/notifications'

import {NOW} from 'src/logs/constants'

import {
  getSourceAndPopulateNamespacesAsync,
  setNamespaceAsync,
  addFilter,
  removeFilter,
  changeFilter,
  clearFilters,
  setSearchStatus,
  setConfig,
} from 'src/logs/actions'
import {getSourcesAsync} from 'src/shared/actions/sources'
import LogsHeader from 'src/logs/components/LogsHeader'
import LoadingStatus from 'src/logs/components/loading_status/LoadingStatus'
import SearchBar from 'src/logs/components/LogsSearchBar'
import FilterBar from 'src/logs/components/logs_filter_bar/LogsFilterBar'
import {Source} from 'src/types/v2'
import {Namespace} from 'src/types'

import {Filter, LogConfig, SearchStatus} from 'src/types/logs'

interface Props {
  sources: Source[]
  currentSource: Source | null
  currentNamespaces: Namespace[]
  currentNamespace: Namespace
  getSourceAndPopulateNamespaces: typeof getSourceAndPopulateNamespacesAsync
  getSources: () => void
  setNamespaceAsync: (namespace: Namespace) => void
  addFilter: (filter: Filter) => void
  removeFilter: (id: string) => void
  changeFilter: (id: string, operator: string, value: string) => void
  clearFilters: () => void
  updateConfig: typeof setConfig
  router: InjectedRouter
  filters: Filter[]
  logConfig: LogConfig
  searchStatus: SearchStatus
  setSearchStatus: typeof setSearchStatus
}

interface State {
  liveUpdating: boolean
}

const RELATIVE_TIME = 0

class LogsPage extends Component<Props, State> {
  constructor(props: Props) {
    super(props)

    this.state = {
      liveUpdating: false,
    }
  }

  public async componentDidUpdate() {
    const {router} = this.props
    if (!this.props.sources || this.props.sources.length === 0) {
      return router.push(`/manage-sources?redirectPath=${location.pathname}`)
    }
  }

  public async componentDidMount() {
    try {
      await this.props.getSources()
      await this.setCurrentSource()
    } catch (e) {
      console.error('Failed to get sources and namespaces for logs')
    }
  }

  public render() {
    const {filters, searchStatus} = this.props

    return (
      <>
        <div className="page">
          {this.header}
          <div className="page-contents logs-viewer">
            <SearchBar
              onSearch={this.handleSubmitSearch}
              customTime={null}
              relativeTime={RELATIVE_TIME}
              onChooseCustomTime={this.handleChooseCustomTime}
              onChooseRelativeTime={this.handleChooseRelativeTime}
            />
            <FilterBar
              filters={filters || []}
              onDelete={this.handleFilterDelete}
              onFilterChange={this.handleFilterChange}
              onClearFilters={this.handleClearFilters}
              onUpdateTruncation={this.handleUpdateTruncation}
              isTruncated={this.isTruncated}
            />
            <LoadingStatus status={searchStatus} lower={0} upper={0} />
          </div>
        </div>
      </>
    )
  }

  private setCurrentSource = async () => {
    if (!this.props.currentSource && this.props.sources.length > 0) {
      const source =
        this.props.sources.find(src => {
          return src.default
        }) || this.props.sources[0]

      return await this.props.getSourceAndPopulateNamespaces(source.links.self)
    }
  }

  private get header(): JSX.Element {
    const {
      sources,
      currentSource,
      currentNamespaces,
      currentNamespace,
    } = this.props

    return (
      <LogsHeader
        liveUpdating={this.isLiveUpdating}
        availableSources={sources}
        onChooseSource={this.handleChooseSource}
        onChooseNamespace={this.handleChooseNamespace}
        currentSource={currentSource}
        currentNamespaces={currentNamespaces}
        currentNamespace={currentNamespace}
        onChangeLiveUpdatingStatus={this.handleChangeLiveUpdatingStatus}
        onShowOptionsOverlay={this.handleToggleOverlay}
      />
    )
  }
  private handleChangeLiveUpdatingStatus = async (): Promise<void> => {
    const {liveUpdating} = this.state

    if (liveUpdating === true) {
      this.setState({liveUpdating: false})
    } else {
      this.handleChooseRelativeTime(NOW)
    }
  }

  private handleSubmitSearch = async (value: string): Promise<void> => {
    searchToFilters(value)
      .reverse()
      .forEach(filter => {
        this.props.addFilter(filter)
      })

    if (this.props.searchStatus === SearchStatus.Loading) {
      this.updateSearchStatus(SearchStatus.UpdatingFilters)
    } else {
      this.updateSearchStatus(SearchStatus.Loading)
    }
  }

  private handleFilterDelete = (id: string): void => {
    this.props.removeFilter(id)
    this.updateSearchStatus(SearchStatus.UpdatingFilters)
  }

  private handleFilterChange = async (
    id: string,
    operator: string,
    value: string
  ): Promise<void> => {
    this.props.changeFilter(id, operator, value)
    this.updateSearchStatus(SearchStatus.UpdatingFilters)
  }

  private updateSearchStatus(status: SearchStatus) {
    if (this.props.searchStatus !== SearchStatus.SourceError) {
      this.props.setSearchStatus(status)
    }
  }

  private handleClearFilters = async (): Promise<void> => {
    this.props.clearFilters()
  }

  private handleChooseSource = async (sourceID: string) => {
    const source = this.props.sources.find(s => s.id === sourceID)
    this.props.setSearchStatus(SearchStatus.Clearing)
    await this.props.getSourceAndPopulateNamespaces(source.links.self)
  }

  private handleChooseNamespace = async (namespace: Namespace) => {
    this.props.setSearchStatus(SearchStatus.Clearing)
    await this.props.setNamespaceAsync(namespace)
    this.props.setSearchStatus(SearchStatus.UpdatingNamespace)
  }

  private handleUpdateTruncation = (isTruncated: boolean) => {
    const {logConfig} = this.props

    this.props.updateConfig({
      ...logConfig,
      isTruncated,
    })
  }

  private get isTruncated(): boolean {
    return this.props.logConfig.isTruncated
  }

  private get isLiveUpdating(): boolean {
    return !!this.state.liveUpdating
  }

  /**
   * Handle choosing a custom time
   * @param time the custom date selected
   */
  private handleChooseCustomTime = async (__: string) => {
    this.setState({liveUpdating: false})
    // TODO: handle updating custom time in LogState
  }

  /**
   * Handle choosing a relative time
   * @param time the epoch time selected
   */
  private handleChooseRelativeTime = async (time: number) => {
    if (time === NOW) {
      this.setState({liveUpdating: true})
    } else {
      this.setState({liveUpdating: false})
    }
    // TODO: handle updating time in LogState
  }

  /**
   * Toggle log config options overlay visibilty
   */
  private handleToggleOverlay = (): void => {}
}

const mapStateToProps = ({
  sources,
  logs: {
    currentSource,
    currentNamespaces,
    currentNamespace,
    filters,
    logConfig,
    searchStatus,
  },
}) => ({
  sources,
  currentSource,
  currentNamespaces,
  currentNamespace,
  filters,
  logConfig,
  searchStatus,
})

const mapDispatchToProps = {
  getSourceAndPopulateNamespaces: getSourceAndPopulateNamespacesAsync,
  getSources: getSourcesAsync,
  setNamespaceAsync,
  setSearchStatus,
  addFilter,
  removeFilter,
  changeFilter,
  clearFilters,
  updateConfig: setConfig,
  notify: notifyAction,
}

export default withRouter(
  connect(mapStateToProps, mapDispatchToProps)(LogsPage)
)
