package orch

import (
	"fmt"

	"github.com/kuasar-sandbox/orchestrator/internal/keys"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
)

func validateSandboxCredentialOverrides(profile types.Profile, credentials sandboxcfg.Credentials) error {
	return sandboxcfg.ValidateCredentialsForProfile(profile, credentials)
}

func materializeSandboxCredentials(sb *types.Sandbox, credentials sandboxcfg.Credentials) error {
	if sb == nil {
		return fmt.Errorf("sandbox is required")
	}
	if err := validateSandboxCredentialOverrides(sb.Profile, credentials); err != nil {
		return err
	}

	serviceSecret := credentials.ServiceSecret
	var err error
	if serviceSecret == "" {
		serviceSecret, err = keys.DeriveServiceSecret(sb.APISecret, sb.AuthSandboxID())
		if err != nil {
			return fmt.Errorf("derive service secret: %w", err)
		}
	}
	forwardToken, err := keys.MintForwardAccessToken(serviceSecret, sb.AuthSandboxID())
	if err != nil {
		return fmt.Errorf("mint forward access token: %w", err)
	}

	sb.ServiceSecret = serviceSecret
	sb.ForwardAccessToken = forwardToken
	if sb.Profile == types.ProfileBare {
		sb.EnvdAccessToken = ""
		sb.TrafficAccessToken = ""
		return nil
	}

	sb.EnvdAccessToken = credentials.EnvdAccessToken
	if sb.EnvdAccessToken == "" {
		sb.EnvdAccessToken, err = keys.MintToken()
		if err != nil {
			return fmt.Errorf("mint envd access token: %w", err)
		}
	}
	sb.TrafficAccessToken = credentials.TrafficAccessToken
	if sb.TrafficAccessToken == "" {
		sb.TrafficAccessToken, err = keys.MintToken()
		if err != nil {
			return fmt.Errorf("mint traffic access token: %w", err)
		}
	}
	return nil
}
