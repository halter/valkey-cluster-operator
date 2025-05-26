// A generated module for ValkeyClusterOperator functions
//
// This module has been generated via dagger init and serves as a reference to
// basic module structure as you get started with Dagger.
//
// Two functions have been pre-created. You can modify, delete, or add to them,
// as needed. They demonstrate usage of arguments and return types using simple
// echo and grep commands. The functions can be called from the dagger CLI or
// from one of the SDKs.
//
// The first line in this comment block is a short description line and the
// rest is a long description with more detail on the module's purpose or usage,
// if appropriate. All modules should have a short description.

package main

import (
	"context"
	"dagger/valkey-cluster-operator/internal/dagger"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"strings"
	"time"

	"github.com/blang/semver/v4"
	"gopkg.in/yaml.v3"
)

const GITHUB_ORG string = "halter"

type ValkeyClusterOperator struct {
	kubeconfig *KubeConfig
}

// Returns a container that echoes whatever string argument is provided
func (m *ValkeyClusterOperator) ContainerEcho(stringArg string) *dagger.Container {
	return dag.Container().From("alpine:latest").WithExec([]string{"echo", stringArg})
}

// Returns lines that match a pattern in the files of the provided Directory
func (m *ValkeyClusterOperator) GrepDir(ctx context.Context, directoryArg *dagger.Directory, pattern string) (string, error) {
	return dag.Container().
		From("alpine:latest").
		WithMountedDirectory("/mnt", directoryArg).
		WithWorkdir("/mnt").
		WithExec([]string{"grep", "-R", pattern, "."}).
		Stdout(ctx)
}

// Build the application binary
func (m *ValkeyClusterOperator) BuildManager(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	// +default="linux/amd64"
	platform dagger.Platform,
) *dagger.File {

	parts := strings.Split(string(platform), "/")
	goos, goarch := parts[0], parts[1]

	builder := dag.Container().
		From("golang:1.24").
		WithWorkdir("/workspace").
		WithFile("/workspace/go.mod", source.File("go.mod")).
		WithFile("/workspace/go.sum", source.File("go.sum")).
		WithFile("/workspace/cmd/main.go", source.File("cmd/main.go")).
		WithDirectory("/workspace/api", source.Directory("api")).
		WithDirectory("/workspace/internal", source.Directory("internal")).
		WithEnvVariable("CGO_ENABLED", "0").
		WithEnvVariable("GOOS", goos).
		WithEnvVariable("GOARCH", goarch).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod-124-"+string(platform))).
		WithEnvVariable("GOMODCACHE", "/go/pkg/mod").
		WithMountedCache("/go/build-cache", dag.CacheVolume("go-build-124-"+string(platform))).
		WithEnvVariable("GOCACHE", "/go/build-cache").
		WithExec([]string{"go", "mod", "download"}).
		WithExec([]string{"go", "build", "-o", "manager", "cmd/main.go"})

	return builder.File("/workspace/manager")
}

// Publish docker container
func (m *ValkeyClusterOperator) PublishDocker(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	tag string,
	ghToken *dagger.Secret,
) (string, error) {
	// container registry for the multi-platform image
	imageRepo := "ghcr.io/" + GITHUB_ORG + "/valkey-cluster-operator:" + tag

	platformVariants, err := m.Build(ctx, source)
	if err != nil {
		return "", err
	}

	// publish to registry
	_, err = dag.Container().
		WithRegistryAuth("ghcr.io", GITHUB_ORG, ghToken).
		Publish(ctx, imageRepo, dagger.ContainerPublishOpts{
			PlatformVariants: platformVariants,
		})

	if err != nil {
		return "", err
	}

	return tag, nil
}

// Publish docker container
func (m *ValkeyClusterOperator) PublishValkeyDocker(
	ctx context.Context,
	ghToken *dagger.Secret,
) error {
	// container registry for the multi-platform image
	imageRepo := "ghcr.io/" + GITHUB_ORG + "/valkey:8.0.2"

	platformVariants, err := m.BuildValkeyContainerImage(ctx)
	if err != nil {
		return err
	}

	// publish to registry
	_, err = dag.Container().
		WithRegistryAuth("ghcr.io", GITHUB_ORG, ghToken).
		Publish(ctx, imageRepo, dagger.ContainerPublishOpts{
			PlatformVariants: platformVariants,
		})

	return err
}

// Build the application container
func (m *ValkeyClusterOperator) Build(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
) ([]*dagger.Container, error) {

	var platforms = []dagger.Platform{
		"linux/amd64", // a.k.a. x86_64
		"linux/arm64", // a.k.a. aarch64
	}

	platformVariants := make([]*dagger.Container, 0, len(platforms))
	for _, platform := range platforms {
		manager := m.BuildManager(ctx, source, platform)
		ctr := source.WithFile("manager", manager).DockerBuild(dagger.DirectoryDockerBuildOpts{Platform: platform})
		platformVariants = append(platformVariants, ctr)
	}

	return platformVariants, nil
}

// Build the valkey container
func (m *ValkeyClusterOperator) BuildValkeyContainerImage(
	ctx context.Context,
) ([]*dagger.Container, error) {

	var platforms = []dagger.Platform{
		"linux/amd64", // a.k.a. x86_64
		"linux/arm64", // a.k.a. aarch64
	}
	valkeyVersion := "8.0.2"

	platformVariants := make([]*dagger.Container, 0, len(platforms))
	for _, platform := range platforms {
		opts := dagger.ContainerOpts{Platform: platform}
		valkey := dag.Container(opts).
			From("bitnami/valkey:" + valkeyVersion)
		platformVariants = append(platformVariants, valkey)
	}
	return platformVariants, nil
}

// Unit Tests
func (m *ValkeyClusterOperator) UnitTest(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
) (string, error) {

	return dag.Container().
		From("golang:1.24").
		WithWorkdir("/workspace").
		WithDirectory("/workspace", source).
		WithMountedCache("/workspace-bin", dag.CacheVolume("workspace-bin")).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod-124")).
		WithEnvVariable("GOMODCACHE", "/go/pkg/mod").
		WithMountedCache("/go/build-cache", dag.CacheVolume("go-build-124")).
		WithEnvVariable("GOCACHE", "/go/build-cache").
		WithExec([]string{"make", "test"}).Stdout(ctx)
}

// Start Kind
func (m *ValkeyClusterOperator) StartKind(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
) (string, error) {

	return dag.Container().
		From("golang:1.24").
		WithExec([]string{"apt", "update"}).
		WithExec([]string{"apt", "install", "-y", "docker"}).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod-124")).
		WithEnvVariable("GOMODCACHE", "/go/pkg/mod").
		WithMountedCache("/go/build-cache", dag.CacheVolume("go-build-124")).
		WithEnvVariable("GOCACHE", "/go/build-cache").
		WithExec([]string{"go", "install", "sigs.k8s.io/kind@v0.29.0"}).
		WithExec([]string{"kind", "create", "cluster"}).Stdout(ctx)
}

// E2E Test
func (m *ValkeyClusterOperator) E2eTest(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	sock *dagger.Socket,
) (string, error) {

	container, err := m.BuildTestEnv(ctx, source, sock)
	if err != nil {
		return "", err
	}

	return container.
		WithExec([]string{"make", "test-e2e"}).Stdout(ctx)
}

func getKubectlRelease() (string, error) {
	resp, err := http.Get("https://dl.k8s.io/release/stable.txt")
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	return string(b), err
}

func (m *ValkeyClusterOperator) BuildAndLoadLocally(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	sock *dagger.Socket,
) error {
	platformVariants, err := m.Build(ctx, source)
	if err != nil {
		return err
	}

	for _, ctr := range platformVariants {
		dockerCli := dag.Container().
			From("docker:cli").
			WithUnixSocket("/var/run/docker.sock", sock).
			WithMountedFile("image.tar", ctr.AsTarball())

		// Load the image from the tarball
		out, err := dockerCli.
			WithExec([]string{"docker", "load", "-i", "image.tar"}).
			Stdout(ctx)
		if err != nil {
			return err
		}

		// Add the tag to the image
		platform, err := ctr.Platform(ctx)
		if err != nil {
			return err
		}
		tag := strings.ReplaceAll(string(platform), "/", "-")
		image := strings.TrimSpace(strings.SplitN(out, ":", 2)[1])
		_, err = dockerCli.
			WithExec([]string{"docker", "tag", image, "valkey-cluster-operator:" + tag}).
			Sync(ctx)
		if err != nil {
			return err
		}
	}
	platformVariants, err = m.BuildValkeyContainerImage(ctx)
	if err != nil {
		return err
	}

	for _, ctr := range platformVariants {
		dockerCli := dag.Container().
			From("docker:cli").
			WithUnixSocket("/var/run/docker.sock", sock).
			WithMountedFile("image.tar", ctr.AsTarball())

		// Load the image from the tarball
		out, err := dockerCli.
			WithExec([]string{"docker", "load", "-i", "image.tar"}).
			Stdout(ctx)
		if err != nil {
			return err
		}

		// Add the tag to the image
		platform, err := ctr.Platform(ctx)
		if err != nil {
			return err
		}
		tag := strings.ReplaceAll(string(platform), "/", "-")
		image := strings.TrimSpace(strings.SplitN(out, ":", 2)[1])
		_, err = dockerCli.
			WithExec([]string{"docker", "tag", image, "valkey:" + tag}).
			Sync(ctx)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *ValkeyClusterOperator) BuildTestEnv(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	sock *dagger.Socket,
) (*dagger.Container, error) {

	err := m.BuildAndLoadLocally(ctx, source, sock)
	if err != nil {
		return nil, err
	}

	goCache := dag.CacheVolume("go")
	goModCache := dag.CacheVolume("go-mod")

	kubectlRelease, err := getKubectlRelease()
	if err != nil {
		return nil, err
	}

	container := dag.Container().
		From("golang:1.24").
		WithUnixSocket("/var/run/docker.sock", sock).
		WithExec([]string{"apt", "update"}).
		WithExec([]string{"apt", "install", "-y", "docker.io", "jq"}).
		WithExec([]string{"curl", "-L", "-o", "/usr/local/bin/kubectl", "https://dl.k8s.io/release/" + kubectlRelease + "/bin/linux/amd64/kubectl"}).
		WithExec([]string{"chmod", "+x", "/usr/local/bin/kubectl"}).
		WithMountedCache("/root/.cache/go-build", goCache).
		WithMountedCache("/root/go/pkg/mod", goModCache).
		WithExec([]string{"go", "install", "sigs.k8s.io/kind@v0.29.0"}).
		WithEnvVariable("CACHEBUSTER", time.Now().String()).
		WithExec([]string{"kind", "create", "cluster"}, dagger.ContainerWithExecOpts{Expect: dagger.ReturnTypeAny}).
		WithExec([]string{"kind", "load", "docker-image", "valkey-cluster-operator:linux-amd64"}, dagger.ContainerWithExecOpts{Expect: dagger.ReturnTypeAny}).
		WithDirectory("/src", source).
		WithWorkdir("/src")

	daggerEngineContainerName, _ := container.WithExec([]string{"bash", "-c", `docker ps --format '{{json .}}' | jq -r '. | select(.Image|startswith("registry.dagger.io/engine:")) | .Names'`}).Stdout(ctx)
	daggerEngineContainerName = strings.TrimSpace(daggerEngineContainerName)
	container.WithExec([]string{"bash", "-c", "docker network connect kind " + daggerEngineContainerName + " || true"}).Stdout(ctx)

	data, _ := container.WithExec([]string{"kind", "get", "kubeconfig"}).Stdout(ctx)

	kindControlPlainContainerID, _ := container.WithExec([]string{"bash", "-c", `docker ps --format '{{json .}}' | jq -r '. | select(.Names=="kind-control-plane") | .ID'`}).Stdout(ctx)
	kindControlPlainContainerID = strings.TrimSpace(kindControlPlainContainerID)
	kindControlPlainContainerIP, _ := container.WithExec([]string{"bash", "-c", `docker inspect ` + kindControlPlainContainerID + ` | jq -r '.[0].NetworkSettings.Networks.kind.IPAddress'`}).Stdout(ctx)
	kindControlPlainContainerIP = strings.TrimSpace(kindControlPlainContainerIP)

	kubeConfig := KubeConfig{}
	_ = yaml.Unmarshal([]byte(data), &kubeConfig)

	kubeConfig.Clusters[0].Cluster.Server = "https://" + kindControlPlainContainerIP + ":6443"

	m.kubeconfig = &kubeConfig

	kubeConfigData, err := yaml.Marshal(kubeConfig)
	if err != nil {
		return nil, err
	}

	return container.
		WithNewFile("/root/.kube/config", string(kubeConfigData)).
		WithEnvVariable("KUBEPATCH_HOST", "https://"+kindControlPlainContainerIP+":6443").
		WithEnvVariable("KUBEPATCH_CLUSTER_CA_CERTIFICATE", kubeConfig.Clusters[0].Cluster.CertificateAuthorityData).
		WithEnvVariable("KUBEPATCH_CLIENT_CERTIFICATE", kubeConfig.Users[0].User.ClientCertificateData).
		WithEnvVariable("KUBEPATCH_CLIENT_KEY", kubeConfig.Users[0].User.ClientKeyData), nil
}

// Build the Kubernetes manifests
func (m *ValkeyClusterOperator) BuildManifests(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	// +default="test"
	tag string,
) *dagger.Directory {

	return dag.Container().
		From("golang:1.24").
		WithWorkdir("/workspace").
		WithDirectory("/workspace", source).
		WithMountedCache("/workspace-bin", dag.CacheVolume("workspace-bin")).
		WithMountedCache("/go/pkg/mod", dag.CacheVolume("go-mod-124")).
		WithEnvVariable("GOMODCACHE", "/go/pkg/mod").
		WithMountedCache("/go/build-cache", dag.CacheVolume("go-build-124")).
		WithEnvVariable("GOCACHE", "/go/build-cache").
		WithEnvVariable("IMG", "ghcr.io/"+GITHUB_ORG+"/valkey-cluster-operator:"+tag).
		WithExec([]string{"make", "build-installer"}).Directory("/workspace/dist")
}

func (m *ValkeyClusterOperator) GhCliContainer() *dagger.Container {
	return dag.Container().
		From("alpine").
		WithExec([]string{"apk", "add", "--no-cache", "github-cli"})
}

func (m *ValkeyClusterOperator) CreateGitHubRelease(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	ghToken *dagger.Secret,
) error {

	nextVersion, err := m.GetNextReleaseVersion(ctx, source, ghToken)
	if err != nil {
		return err
	}

	manifestDir := m.BuildManifests(ctx, source, nextVersion)

	tf, err := m.CreateTerraform(ctx, manifestDir.File("install.yaml"))
	if err != nil {
		return err
	}

	_, err = m.GhCliContainer().
		WithDirectory("/manifest", manifestDir).
		WithNewFile("/install.tf", tf).
		WithSecretVariable("GH_TOKEN", ghToken).
		WithExec([]string{"gh", "--repo=" + GITHUB_ORG + "/valkey-cluster-operator", "release", "create", nextVersion, "/manifest/install.yaml", "/install.tf"}).Stdout(ctx)
	if err != nil {
		return err
	}

	_, err = m.PublishDocker(ctx, source, nextVersion, ghToken)
	if err != nil {
		return err
	}
	err = m.PublishValkeyDocker(ctx, ghToken)
	if err != nil {
		return err
	}
	return nil

}

func (m *ValkeyClusterOperator) GetNextReleaseVersion(
	ctx context.Context,
	// +defaultPath="/"
	source *dagger.Directory,
	ghToken *dagger.Secret,
) (string, error) {

	type Releases []struct {
		CreatedAt    time.Time `json:"createdAt"`
		IsDraft      bool      `json:"isDraft"`
		IsLatest     bool      `json:"isLatest"`
		IsPrerelease bool      `json:"isPrerelease"`
		Name         string    `json:"name"`
		PublishedAt  time.Time `json:"publishedAt"`
		TagName      string    `json:"tagName"`
	}

	releasesTxt, err := m.GhCliContainer().
		WithSecretVariable("GH_TOKEN", ghToken).
		WithExec([]string{"gh", "release", "--repo=" + GITHUB_ORG + "/valkey-cluster-operator", "list", "--json", "createdAt,isDraft,isLatest,isPrerelease,name,publishedAt,tagName"}).Stdout(ctx)

	r := Releases{}
	err = json.Unmarshal([]byte(releasesTxt), &r)
	if err != nil {
		return "", err
	}

	for _, rel := range r {
		if rel.IsLatest {
			v := semver.MustParse(strings.TrimPrefix(rel.TagName, "v"))
			err = v.IncrementPatch()
			if err != nil {
				return "", err
			}
			return "v" + v.String(), nil
		}
	}

	return "", fmt.Errorf("Could not get next release version")

}

func (m *ValkeyClusterOperator) CreateTerraform(
	ctx context.Context,
	yamlFile *dagger.File,
) (string, error) {

	yamlStr, err := yamlFile.Contents(ctx)
	if err != nil {
		return "", err
	}

	tf, err := YamlToTf(ctx, yamlStr)
	if err != nil {
		return "", err
	}

	return tf, nil
}

func YamlToTf(ctx context.Context, yamlStr string) (string, error) {
	ctr := dag.Container().
		From("golang:1.24").
		WithExec([]string{"go", "install", "github.com/jrhouston/tfk8s@latest"}).
		WithExec([]string{"sh", "-c", "git clone https://github.com/sl1pm4t/k2tf.git && cd k2tf && go install && cd .. && rm -rf k2tf"})

	builder := strings.Builder{}
	parts := strings.Split(yamlStr, "---\n")

	namespace := ""
	for _, part := range parts {
		tmpfile, err := os.CreateTemp("", "k2tf-*.yaml")
		if err != nil {
			return "", err
		}

		err = os.WriteFile(tmpfile.Name(), []byte(part), 0644)
		if err != nil {
			return "", err
		}

		defer func() {
			err := os.Remove(tmpfile.Name())
			if err != nil {
				fmt.Printf("Error deleting temporary file: %v", err)
			}
		}()

		var obj struct {
			Kind       string `yaml:"kind"`
			ApiVersion string `yaml:"apiVersion"`
			Metadata   struct {
				Name string `yaml:"name,omitempty"`
			} `yaml:"metadata,omitempty"`
		}
		err = yaml.Unmarshal([]byte(part), &obj)
		if err != nil {
			return "", err
		}

		var output string
		if obj.Kind == "CustomResourceDefinition" {
			output, err = ctr.
				WithNewFile("/tmp/file.yaml", part).
				WithExec([]string{"tfk8s", "-f", "/tmp/file.yaml"}).Stdout(ctx)
			if err != nil {
				return "", err
			}
		} else {
			output, err = ctr.
				WithNewFile("/tmp/file.yaml", part).
				WithExec([]string{"k2tf", "-f", "/tmp/file.yaml"}).Stdout(ctx)
			if err != nil {
				return "", err
			}
		}

		if obj.Kind == "Namespace" {
			namespace = obj.Metadata.Name
		}

		builder.WriteString(string(output))
	}

	result := builder.String()

	if namespace != "" {
		re := regexp.MustCompile(fmt.Sprintf(`namespace\s*=\s*"%s"`, namespace))
		result = re.ReplaceAllString(result, fmt.Sprintf("namespace = kubernetes_namespace.%s.metadata[0].name", strings.ReplaceAll(namespace, "-", "_")))
	}

	return dag.Container().
		From("hashicorp/terraform:1.12").
		WithWorkdir("/workspace").
		WithNewFile("/workspace/install.tf", result).
		WithExec([]string{"terraform", "fmt"}).
		File("/workspace/install.tf").
		Contents(ctx)
}
