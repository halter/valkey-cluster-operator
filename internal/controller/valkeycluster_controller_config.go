package controller

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	cachev1alpha1 "github.com/halter/valkey-cluster-operator/api/v1alpha1"
	"github.com/valkey-io/valkey-go"
)

// Replication-tuning default bounds. Sizing rationale lives in the README's
// "Operator-managed defaults" table; values sized per the SP-1527 incident.
const (
	// managedDefaultReplBacklogMin is Valkey's compiled-in default (10mb).
	managedDefaultReplBacklogMin = int64(10 * 1024 * 1024)
	managedDefaultReplBacklogMax = int64(512 * 1024 * 1024)
	managedDefaultReplicaHardMin = int64(64 * 1024 * 1024)
	managedDefaultReplicaHardMax = int64(4 * 1024 * 1024 * 1024)
	// managedDefaultReplicaSoftSeconds is the soft-limit window (compiled
	// default is 60s).
	managedDefaultReplicaSoftSeconds = 120
)

// managedDefaultParameters returns the operator-wide replication-tuning
// defaults, derived from the cluster's pod memory limit. Sizing rationale:
// README, "Operator-managed defaults". They are returned only when:
//
//   - spec.valkeyConfig.rawConfig is empty. A raw config replaces the
//     operator's default config file wholesale — expert mode — so the
//     operator does not layer defaults it cannot see overridden.
//   - the image tag parses as Valkey >= 8.0, the version that introduced
//     dual-channel-replication-enabled (absent from valkey 7.2's valkey.conf,
//     documented in 8.0.5's). Rendering a directive an older server does not
//     recognise into the config file would stop pods from booting (verified
//     on ghcr.io/halter/valkey-server:8.0.5: an unknown file directive exits
//     1 with "Bad directive or wrong number of arguments"). Unparseable tags
//     (digest pins, non-numeric tags) get no defaults — the safe direction:
//     behaviour is unchanged from operator versions before this.
//
// spec.valkeyConfig.parameters entries are appended after these (both in the
// config file and in the live-apply order), so a per-cluster value always
// overrides the fleet default.
func managedDefaultParameters(valkeyCluster *cachev1alpha1.ValkeyCluster) []cachev1alpha1.ValkeyConfigParameter {
	if valkeyCluster.Spec.ValkeyConfig != nil && valkeyCluster.Spec.ValkeyConfig.RawConfig != "" {
		return nil
	}
	if !imageSupportsManagedDefaults(valkeyCluster.Spec.Image) {
		return nil
	}

	defaults := []cachev1alpha1.ValkeyConfigParameter{
		{Name: "dual-channel-replication-enabled", Value: "yes"},
	}

	memoryLimit := podMemoryLimitBytes(valkeyCluster)
	if memoryLimit <= 0 {
		// No sizing basis — leave the sized directives at compiled defaults.
		return defaults
	}

	backlog := min(max(memoryLimit/16, managedDefaultReplBacklogMin), managedDefaultReplBacklogMax)

	// The replica hard limit must stay above repl-backlog-size (a replica
	// limit below it is ignored, per valkey.conf 8.0.5); with memory/16 vs
	// memory/2 and these clamp ranges, backlog <= hard holds for every
	// memory value.
	hard := min(max(memoryLimit/2, managedDefaultReplicaHardMin), managedDefaultReplicaHardMax)
	soft := hard / 2

	return append(defaults,
		cachev1alpha1.ValkeyConfigParameter{Name: "repl-backlog-size", Value: strconv.FormatInt(backlog, 10)},
		cachev1alpha1.ValkeyConfigParameter{Name: "client-output-buffer-limit", Value: fmt.Sprintf("replica %d %d %d", hard, soft, managedDefaultReplicaSoftSeconds)},
	)
}

// imageSupportsManagedDefaults reports whether the image tag positively
// parses as Valkey >= 8.0. Tags like "ghcr.io/halter/valkey-server:8.0.5",
// "ghcr.io/halter/valkey:8.0.2" and "...:8.1.9" qualify. Anything that does
// not parse (no tag, digest pins, non-numeric tags) is treated as
// unsupported so no directive lands in a config file that an unknown server
// version might refuse to boot with.
func imageSupportsManagedDefaults(image string) bool {
	idx := strings.LastIndex(image, ":")
	if idx < 0 || strings.Contains(image[idx:], "/") {
		return false
	}
	tag := strings.TrimPrefix(image[idx+1:], "v")
	parts := strings.SplitN(tag, ".", 3)
	if len(parts) < 2 {
		return false
	}
	major, err := strconv.Atoi(parts[0])
	if err != nil {
		return false
	}
	if _, err := strconv.Atoi(parts[1]); err != nil {
		return false
	}
	return major >= 8
}

func podMemoryLimitBytes(valkeyCluster *cachev1alpha1.ValkeyCluster) int64 {
	if valkeyCluster.Spec.Resources == nil {
		return 0
	}
	return valkeyCluster.Spec.Resources.Limits.Memory().Value()
}

// managedConfigEntries returns the ordered list of config directives the
// operator actively manages on running pods: the operator-wide defaults
// followed by the spec's valkeyConfig.parameters. Entries are applied in
// order; for directives where the same name appears more than once (e.g.
// client-output-buffer-limit once per client class), later entries win for
// the class/value they address, matching valkey.conf semantics — so spec
// parameters override the defaults. getValkeyConfigContent renders the same
// entries in the same order into the config file, keeping the live-applied
// state and the restart state identical.
func managedConfigEntries(valkeyCluster *cachev1alpha1.ValkeyCluster) []cachev1alpha1.ValkeyConfigParameter {
	entries := managedDefaultParameters(valkeyCluster)
	if valkeyCluster.Spec.ValkeyConfig != nil {
		entries = append(entries, valkeyCluster.Spec.ValkeyConfig.Parameters...)
	}
	return entries
}

// reconcileValkeyConfig live-applies the managed config directives to every
// running pod via CONFIG GET -> compare -> CONFIG SET, generalising the
// reconcileAuth pattern. The config file (ConfigMap) only takes effect on pod
// restart — and the ConfigMap is mounted via subPath, so running pods never
// even see file updates. Without the live apply, a spec.valkeyConfig change
// goes nowhere until pods happen to restart, and manual CONFIG SET drift
// (e.g. incident remediation) silently reverts on any restart with nothing
// converging it back. Running this every reconcile gives both directions:
// declarative changes reach running pods without restarts, and runtime drift
// on managed directives converges back to spec.
//
// Directives that the server rejects at runtime (immutable configs) are
// collected per pod and returned so the caller can drive a health-gated
// rolling restart; pods restart onto the already-updated config file.
// Directives unknown to the running server version are skipped with a warning
// — a restart would not help and the unknown directive in the config file
// would prevent the pod from booting at all.
func (r *ValkeyClusterReconciler) reconcileValkeyConfig(ctx context.Context, valkeyCluster *cachev1alpha1.ValkeyCluster) ([]corev1.Pod, error) {
	logger := log.FromContext(ctx)

	entries := managedConfigEntries(valkeyCluster)
	if len(entries) == 0 {
		return nil, nil
	}

	names := make([]string, 0, len(entries))
	seen := make(map[string]bool, len(entries))
	for _, e := range entries {
		if !seen[e.Name] {
			seen[e.Name] = true
			names = append(names, e.Name)
		}
	}

	podList := &corev1.PodList{}
	listOpts := []client.ListOption{
		client.InNamespace(valkeyCluster.Namespace),
		client.MatchingLabels(labelsForValkeyCluster(valkeyCluster.Name)),
	}
	if err := r.List(ctx, podList, listOpts...); err != nil {
		return nil, err
	}

	// Persistent conditions (unknown directives, rejected values, restart
	// required) are aggregated and evented once per directive per reconcile
	// rather than once per pod: client-go's event spam filter budgets by
	// involved object, so per-pod repeats would starve the cluster's whole
	// event stream.
	unknownDirectives := map[string]int{}
	rejectedDirectives := map[string]string{}
	restartDirectives := map[string]string{}

	var podsNeedingRestart []corev1.Pod
	for _, pod := range podList.Items {
		if pod.DeletionTimestamp != nil || pod.Status.Phase != corev1.PodRunning || pod.Status.PodIP == "" {
			continue
		}
		valkeyClient, err := r.NewValkeyClient(ctx, valkeyCluster, pod.Status.PodIP, VALKEY_PORT)
		if err != nil {
			return nil, fmt.Errorf("failed to create valkey client for pod %s: %w", pod.Name, err)
		}
		defer valkeyClient.Close()

		current, err := valkeyClient.Do(ctx, valkeyClient.B().ConfigGet().Parameter(names...).Build()).AsStrMap()
		if err != nil {
			return nil, fmt.Errorf("failed to get config from pod %s: %w", pod.Name, err)
		}

		needsRestart := false
		for _, e := range entries {
			currentValue, known := current[e.Name]
			if !known {
				// CONFIG GET omits directives this server version does not
				// recognise. Don't restart for it: the directive is also in
				// the config file, and an unknown file directive would stop
				// the recreated pod from booting.
				unknownDirectives[e.Name]++
				continue
			}
			if valkeyConfigValueEqual(e.Name, e.Value, currentValue) {
				continue
			}
			err := valkeyClient.Do(ctx, valkeyClient.B().ConfigSet().
				ParameterValue().
				ParameterValue(e.Name, e.Value).
				Build()).Error()
			if err == nil {
				logger.Info("Applied config directive to running pod",
					"pod", pod.Name, "directive", e.Name, "value", e.Value)
				r.Recorder.Event(valkeyCluster, "Normal", "ConfigUpdated",
					fmt.Sprintf("Applied %s to pod %s", e.Name, pod.Name))
				continue
			}
			if _, isServerErr := valkey.IsValkeyErr(err); isServerErr {
				if isConfigSetRestartableError(err) {
					restartDirectives[e.Name] = err.Error()
					needsRestart = true
				} else {
					rejectedDirectives[e.Name] = err.Error()
				}
				continue
			}
			return nil, fmt.Errorf("failed to set config %s on pod %s: %w", e.Name, pod.Name, err)
		}
		if needsRestart {
			podsNeedingRestart = append(podsNeedingRestart, pod)
		}
	}

	for name, count := range unknownDirectives {
		logger.Info("Skipping config directive unknown to running server",
			"directive", name, "pods", count)
		r.Recorder.Event(valkeyCluster, "Warning", "ConfigDirectiveUnknown",
			fmt.Sprintf("Directive %q is not recognised by the valkey server (%d pods); skipping live apply", name, count))
	}
	for name, errText := range rejectedDirectives {
		logger.Info("Config directive rejected by server, not restarting",
			"directive", name, "error", errText)
		r.Recorder.Event(valkeyCluster, "Warning", "ConfigValueRejected",
			fmt.Sprintf("CONFIG SET %s rejected (%s); fix the value in spec.valkeyConfig — pods restarted with this value in the config file may fail to boot", name, errText))
	}
	for name, errText := range restartDirectives {
		logger.Info("Config directive not settable at runtime, scheduling rolling restart",
			"directive", name, "error", errText, "pods", len(podsNeedingRestart))
		r.Recorder.Event(valkeyCluster, "Warning", "ConfigRequiresRestart",
			fmt.Sprintf("%s cannot be set at runtime (%s); applying via health-gated rolling restart of %d pods", name, errText, len(podsNeedingRestart)))
	}

	return podsNeedingRestart, nil
}

// isConfigSetRestartableError reports whether a CONFIG SET server rejection
// means the directive can only be applied by restarting the pod onto the
// updated config file. Valkey formats these rejections as "... can't set
// immutable config" and "... can't set protected config" — both file-settable
// classes (the errstr ternary in configSetCommand,
// https://github.com/valkey-io/valkey/blob/8.0.5/src/config.c).
// Every other rejection is treated as a value error, for which the caller
// must NOT restart: the same value sits in the rendered config file, and
// valkey 8.0.5 refuses to boot on an invalid file directive (verified against
// ghcr.io/halter/valkey-server:8.0.5), so restarting would trade a
// running-but-stale pod for a crash-looping one.
func isConfigSetRestartableError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "can't set immutable config") ||
		strings.Contains(msg, "can't set protected config")
}

// valkeyConfigValueEqual reports whether a desired config value and the value
// CONFIG GET returned are semantically equal. CONFIG GET canonicalises values
// (memory sizes come back as plain byte counts, client-output-buffer-limit
// comes back as the full three-class string), so a plain string compare would
// report perpetual mismatches and re-SET on every reconcile.
func valkeyConfigValueEqual(name, desired, current string) bool {
	d := strings.TrimSpace(desired)
	c := strings.TrimSpace(current)
	if strings.EqualFold(d, c) {
		return true
	}
	if name == "client-output-buffer-limit" {
		return clientOutputBufferLimitSatisfied(d, c)
	}
	if dv, ok := parseValkeyMemory(d); ok {
		if cv, ok := parseValkeyMemory(c); ok {
			return dv == cv
		}
	}
	return false
}

// parseValkeyMemory parses a value in valkey.conf memory notation into bytes.
// Units per the "Note on units" header of
// https://github.com/valkey-io/valkey/blob/8.0.5/valkey.conf:
// 1k => 1000, 1kb => 1024, 1m => 1000*1000, 1mb => 1024*1024,
// 1g => 1000*1000*1000, 1gb => 1024*1024*1024, case insensitive.
// Round-trip verified against ghcr.io/halter/valkey-server:8.0.5:
// CONFIG SET repl-backlog-size 512mb -> CONFIG GET returns "536870912".
func parseValkeyMemory(s string) (int64, bool) {
	s = strings.ToLower(strings.TrimSpace(s))
	mult := int64(1)
	switch {
	case strings.HasSuffix(s, "kb"):
		mult, s = 1024, strings.TrimSuffix(s, "kb")
	case strings.HasSuffix(s, "mb"):
		mult, s = 1024*1024, strings.TrimSuffix(s, "mb")
	case strings.HasSuffix(s, "gb"):
		mult, s = 1024*1024*1024, strings.TrimSuffix(s, "gb")
	case strings.HasSuffix(s, "k"):
		mult, s = 1000, strings.TrimSuffix(s, "k")
	case strings.HasSuffix(s, "m"):
		mult, s = 1000*1000, strings.TrimSuffix(s, "m")
	case strings.HasSuffix(s, "g"):
		mult, s = 1000*1000*1000, strings.TrimSuffix(s, "g")
	}
	n, err := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	if err != nil {
		return 0, false
	}
	return n * mult, true
}

type clientOutputBufferLimit struct {
	hard, soft, seconds int64
}

// parseClientOutputBufferLimitClasses parses "class hard soft seconds"
// groups. Class names are normalised so "slave" and "replica" compare equal
// (CONFIG GET reports the class as "slave" for compatibility; config files
// may use either spelling).
func parseClientOutputBufferLimitClasses(s string) (map[string]clientOutputBufferLimit, bool) {
	fields := strings.Fields(s)
	if len(fields) == 0 || len(fields)%4 != 0 {
		return nil, false
	}
	out := make(map[string]clientOutputBufferLimit, len(fields)/4)
	for i := 0; i < len(fields); i += 4 {
		class := strings.ToLower(fields[i])
		if class == "slave" {
			class = "replica"
		}
		hard, ok := parseValkeyMemory(fields[i+1])
		if !ok {
			return nil, false
		}
		soft, ok := parseValkeyMemory(fields[i+2])
		if !ok {
			return nil, false
		}
		seconds, err := strconv.ParseInt(fields[i+3], 10, 64)
		if err != nil {
			return nil, false
		}
		out[class] = clientOutputBufferLimit{hard: hard, soft: soft, seconds: seconds}
	}
	return out, true
}

// clientOutputBufferLimitSatisfied reports whether every class present in the
// desired value matches the corresponding class in the current (canonical,
// all-classes) value. Classes not mentioned in the desired value are left to
// their current setting, mirroring how a config-file line for one class does
// not affect the others.
func clientOutputBufferLimitSatisfied(desired, current string) bool {
	des, ok := parseClientOutputBufferLimitClasses(desired)
	if !ok {
		return false
	}
	cur, ok := parseClientOutputBufferLimitClasses(current)
	if !ok {
		return false
	}
	for class, want := range des {
		got, present := cur[class]
		if !present || got != want {
			return false
		}
	}
	return true
}
