// Libraries
import React, {PureComponent} from 'react'
import Dygraph from 'src/shared/components/dygraph/Dygraph'
import DygraphCell from 'src/shared/components/DygraphCell'
import DygraphTransformation from 'src/shared/components/DygraphTransformation'

// Components
import {ErrorHandlingWith} from 'src/shared/decorators/errors'
import InvalidData from 'src/shared/components/InvalidData'

// Types
import {Options} from 'src/external/dygraph'
import {StackedView} from 'src/types/v2/dashboards'
import {TimeRange} from 'src/types/v2'
import {FluxTable, RemoteDataState} from 'src/types'

interface Props {
  loading: RemoteDataState
  properties: StackedView
  timeRange: TimeRange
  tables: FluxTable[]
  viewID: string
  staticLegend: boolean
  onZoom: () => void
  handleSetHoverTime: () => void
}

@ErrorHandlingWith(InvalidData)
class Stacked extends PureComponent<Props> {
  public static defaultProps: Partial<Props> = {
    staticLegend: false,
  }

  public render() {
    const {
      tables,
      viewID,
      onZoom,
      loading,
      timeRange,
      properties,
      staticLegend,
      handleSetHoverTime,
    } = this.props

    const {axes, type, colors, queries} = properties

    return (
      <DygraphTransformation tables={tables}>
        {({labels, dygraphsData}) => (
          <DygraphCell loading={loading}>
            <Dygraph
              type={type}
              axes={axes}
              viewID={viewID}
              colors={colors}
              onZoom={onZoom}
              labels={labels}
              queries={queries}
              options={this.options}
              timeRange={timeRange}
              timeSeries={dygraphsData}
              staticLegend={staticLegend}
              handleSetHoverTime={handleSetHoverTime}
            />
          </DygraphCell>
        )}
      </DygraphTransformation>
    )
  }

  private get options(): Partial<Options> {
    return {
      rightGap: 0,
      yRangePad: 10,
      labelsKMB: true,
      fillGraph: true,
      axisLabelWidth: 60,
      animatedZooms: true,
      drawAxesAtZero: true,
      axisLineColor: '#383846',
      gridLineColor: '#383846',
      connectSeparatedPoints: true,
      stackedGraph: true,
    }
  }
}

export default Stacked
