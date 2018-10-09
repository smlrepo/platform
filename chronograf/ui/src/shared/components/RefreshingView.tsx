// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'
import {connect} from 'react-redux'
import _ from 'lodash'

// Components
import LineGraph from 'src/shared/components/LineGraph'
import StepPlot from 'src/shared/components/StepPlot'
import Stacked from 'src/shared/components/Stacked'
import GaugeChart from 'src/shared/components/GaugeChart'
import SingleStat from 'src/shared/components/SingleStat'
import TimeSeries from 'src/shared/components/time_series/TimeSeries'
import SingleStatTransform from 'src/shared/components/SingleStatTransform'
import TimeMachineTables from 'src/shared/components/tables/TimeMachineTables'

// Constants
import {emptyGraphCopy} from 'src/shared/copy/cell'
// import {DEFAULT_TIME_FORMAT} from 'src/dashboards/constants'

// Actions
import {setHoverTime} from 'src/dashboards/actions/v2/hoverTime'

// Types
import {TimeRange, Template} from 'src/types'
import {DashboardQuery} from 'src/types/v2/dashboards'
import {
  RefreshingViewProperties,
  ViewType,
  LineView,
  SingleStatView,
} from 'src/types/v2/dashboards'

interface Props {
  link: string
  timeRange: TimeRange
  templates: Template[]
  viewID: string
  inView: boolean
  timeFormat: string
  autoRefresh: number
  manualRefresh: number
  staticLegend: boolean
  onZoom: () => void
  editQueryStatus: () => void
  onSetResolution: () => void
  grabDataForDownload: () => void
  handleSetHoverTime: () => void
  properties: RefreshingViewProperties
}

class RefreshingView extends PureComponent<Props & WithRouterProps> {
  public static defaultProps: Partial<Props> = {
    inView: true,
    manualRefresh: 0,
    staticLegend: false,
  }

  public render() {
    const {
      link,
      inView,
      onZoom,
      viewID,
      timeRange,
      templates,
      properties,
      staticLegend,
      manualRefresh,
      handleSetHoverTime,
    } = this.props

    if (!properties.queries.length) {
      return (
        <div className="graph-empty">
          <p data-test="data-explorer-no-results">{emptyGraphCopy}</p>
        </div>
      )
    }

    return (
      <TimeSeries
        link={link}
        inView={inView}
        queries={this.queries}
        templates={templates}
        key={manualRefresh}
      >
        {({tables, loading}) => {
          switch (properties.type) {
            case ViewType.SingleStat:
              return (
                <SingleStatTransform tables={tables}>
                  {stat => <SingleStat stat={stat} properties={properties} />}
                </SingleStatTransform>
              )
            case ViewType.Table:
              return (
                <TimeMachineTables tables={tables} properties={properties} />
              )
            case ViewType.Gauge:
              return (
                <GaugeChart
                  tables={tables}
                  key={manualRefresh}
                  properties={properties}
                />
              )
            case ViewType.Line:
              return (
                <LineGraph
                  tables={tables}
                  viewID={viewID}
                  onZoom={onZoom}
                  loading={loading}
                  timeRange={timeRange}
                  properties={properties}
                  staticLegend={staticLegend}
                  handleSetHoverTime={handleSetHoverTime}
                />
              )
            case ViewType.LinePlusSingleStat:
              const lineProperties = {
                ...properties,
                type: ViewType.Line,
              } as LineView

              const singleStatProperties = {
                ...properties,
                type: ViewType.SingleStat,
              } as SingleStatView

              return (
                <LineGraph
                  tables={tables}
                  viewID={viewID}
                  onZoom={onZoom}
                  loading={loading}
                  timeRange={timeRange}
                  properties={lineProperties}
                  staticLegend={staticLegend}
                  handleSetHoverTime={handleSetHoverTime}
                >
                  <SingleStatTransform tables={tables}>
                    {stat => (
                      <SingleStat
                        stat={stat}
                        properties={singleStatProperties}
                      />
                    )}
                  </SingleStatTransform>
                </LineGraph>
              )
            case ViewType.StepPlot:
              return (
                <StepPlot
                  tables={tables}
                  viewID={viewID}
                  onZoom={onZoom}
                  loading={loading}
                  timeRange={timeRange}
                  properties={properties}
                  staticLegend={staticLegend}
                  handleSetHoverTime={handleSetHoverTime}
                />
              )
            case ViewType.Stacked:
              return (
                <Stacked
                  tables={tables}
                  viewID={viewID}
                  onZoom={onZoom}
                  loading={loading}
                  timeRange={timeRange}
                  properties={properties}
                  staticLegend={staticLegend}
                  handleSetHoverTime={handleSetHoverTime}
                />
              )
            default:
              return <div>YO!</div>
          }
        }}
      </TimeSeries>
    )
  }

  private get queries(): DashboardQuery[] {
    const {properties} = this.props
    const {type, queries} = properties

    if (type === ViewType.SingleStat) {
      return [queries[0]]
    }

    if (type === ViewType.Gauge) {
      return [queries[0]]
    }

    return queries
  }
}

const mstp = ({sources, routing}): Partial<Props> => {
  const sourceID = routing.locationBeforeTransitions.query.sourceID
  const source = sources.find(s => s.id === sourceID)
  const link = source.links.query

  return {
    link,
  }
}

const mdtp = {
  handleSetHoverTime: setHoverTime,
}

export default connect(mstp, mdtp)(withRouter(RefreshingView))
