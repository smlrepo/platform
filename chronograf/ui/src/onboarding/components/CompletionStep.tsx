// Libraries
import React, {PureComponent} from 'react'
import {withRouter, WithRouterProps} from 'react-router'

// Components
import {ErrorHandling} from 'src/shared/decorators/errors'

// Types
import {Button, ComponentColor, ComponentSize} from 'src/clockface'
import {OnboardingStepProps} from 'src/onboarding/containers/OnboardingWizard'

@ErrorHandling
class CompletionStep extends PureComponent<
  OnboardingStepProps & WithRouterProps
> {
  public render() {
    return (
      <div className="onboarding-step">
        <div className="splash-logo secondary" />
        <h3 className="wizard-step-title">Setup Complete! </h3>
        <p>This is completion step</p>
        <div className="wizard-button-bar">
          <Button
            color={ComponentColor.Default}
            text="Back"
            size={ComponentSize.Medium}
            onClick={this.handleDecrement}
          />
          <Button
            color={ComponentColor.Success}
            text="Go to status dashboard"
            size={ComponentSize.Large}
            onClick={this.handleComplete}
          />
        </div>
      </div>
    )
  }

  private handleComplete = () => {
    const {router, completeSetup} = this.props
    completeSetup()
    router.push(`/manage-sources`)
  }
  private handleDecrement = () => {
    const {handleSetCurrentStep, currentStepIndex} = this.props
    handleSetCurrentStep(currentStepIndex - 1)
  }
}

export default withRouter(CompletionStep)
