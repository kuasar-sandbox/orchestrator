package builder

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/orchestrator/internal/sandboxcfg"
	"github.com/kuasar-sandbox/orchestrator/internal/taskartifact"
	"github.com/kuasar-sandbox/orchestrator/internal/types"
	rtconfig "github.com/kuasar-sandbox/sandboxer/pkg/config"
)

// This uses the real strict image reader, Manifest service, publication
// carriers and E assembler. No VM or systemd execution is claimed by this test.
func TestE2BEnvironmentUsesSameFinalImageForBuildAndFreshCreate(t *testing.T) {
	for _, bundle := range []bool{false, true} {
		t.Run(fmt.Sprintf("bundle=%t", bundle), func(t *testing.T) {
			f := newPublicationFixture(t)
			publisher := f.pipeline(t, "product-env-image", f.localImageRef, nil, bundle)
			ref, err := publisher.publishImageTarget()
			if err != nil {
				t.Fatal(err)
			}
			projection := configsock.ImageLaunchProjection{ImageRef: ref, ManifestConfig: f.manifestPath, RefLocations: publisher.spec.RefLocations}
			environment, err := taskartifact.LoadImageEnvironment(context.Background(), projection)
			if err != nil {
				t.Fatal(err)
			}
			if environment.ImageRef != ref || len(environment.ConfigSHA256) != 64 || environment.Env["IMAGE"] != "yes" {
				t.Fatalf("wrong image environment: %+v", environment)
			}
			p := f.pipeline(t, "product-env-definition", ref, publisher.spec.RefLocations, bundle)
			p.profile, p.spec.Profile = types.ProfileE2B, string(types.ProfileE2B)
			p.spec.SandboxSpec.Launch = &rtconfig.LaunchConfig{Systemd: "provided"}
			p.spec.SandboxEnv = map[string]string{"INSTANCE": "yes"}
			document, err := p.buildColdConfigYAML()
			if err != nil {
				t.Fatal(err)
			}
			phaseC, err := rtconfig.LoadConfigBytes(document)
			if err != nil {
				t.Fatal(err)
			}
			check := func(launch rtconfig.LaunchConfig) {
				t.Helper()
				if launch.Plugin[0].Env["IMAGE"] != "yes" || launch.Plugin[0].Env["INSTANCE"] != "yes" || !strings.HasPrefix(launch.Plugin[0].Env["PATH"], "/run/sandbox-runtime/systemd/bin:") {
					t.Fatalf("image defaults absent from product plugin: %+v", launch.Plugin[0])
				}
			}
			check(phaseC.Launch)
			image, err := p.openCurrentImage()
			if err != nil {
				t.Fatal(err)
			}
			defer image.Close()
			cfg, portable, err := p.sandboxAssembly(image)
			if err != nil {
				t.Fatal(err)
			}
			check(cfg.Launch)
			if portable.Launch.Plugin[0].Env["IMAGE"] != "yes" || !portable.Launch.SystemdPathDefault {
				t.Fatal("top-level E lost product environment or default PATH origin")
			}
			// The image fast path applies the same defaults to the ordinary
			// rendered host document and checks the immutable boot identity.
			fresh, err := sandboxcfg.ProjectImageLaunch(document, ref, environment.Env)
			if err != nil {
				t.Fatal(err)
			}
			created, err := rtconfig.LoadConfigBytes(fresh)
			if err != nil {
				t.Fatal(err)
			}
			check(created.Launch)
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if _, err = taskartifact.LoadImageEnvironment(ctx, projection); err == nil {
				t.Fatal("canceled image read proceeded")
			}
		})
	}
}
