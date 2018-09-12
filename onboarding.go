package platform

import "context"

// OnboardingDefaults is a group of elements required for first run.
type OnboardingDefaults struct {
	User   User
	Org    Organization
	Bucket Bucket
	Auth   Authorization
}

// OnboardingService represents a service for the first run.
type OnboardingService interface {
	// IsOnboarding determine if it is onboarding.
	IsOnboarding() bool
	// Generate OnboardingDefaults.
	Generate(ctx context.Context) (*OnboardingDefaults, error)
}
