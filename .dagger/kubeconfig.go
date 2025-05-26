package main

type KubeConfig struct {
	APIVersion     string           `yaml:"apiVersion"`
	Clusters       []ClusterElement `yaml:"clusters"`
	Contexts       []ContextElement `yaml:"contexts"`
	CurrentContext string           `yaml:"current-context"`
	Kind           string           `yaml:"kind"`
	Preferences    Preferences      `yaml:"preferences"`
	Users          []UserElement    `yaml:"users"`
}

type ClusterElement struct {
	Cluster ClusterCluster `yaml:"cluster"`
	Name    string         `yaml:"name"`
}

type ClusterCluster struct {
	CertificateAuthorityData string `yaml:"certificate-authority-data"`
	Server                   string `yaml:"server"`
}

type ContextElement struct {
	Context ContextContext `yaml:"context"`
	Name    string         `yaml:"name"`
}

type ContextContext struct {
	Cluster string `yaml:"cluster"`
	User    string `yaml:"user"`
}

type Preferences struct {
}

type UserElement struct {
	Name string   `yaml:"name"`
	User UserUser `yaml:"user"`
}

type UserUser struct {
	ClientCertificateData string `yaml:"client-certificate-data"`
	ClientKeyData         string `yaml:"client-key-data"`
}
