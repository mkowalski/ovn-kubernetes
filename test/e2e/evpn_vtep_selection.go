// SPDX-FileCopyrightText: Copyright The OVN-Kubernetes Contributors
// SPDX-License-Identifier: Apache-2.0

package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"

	vtepv1 "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/vtep/v1"
	vtepclientset "github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/crd/vtep/v1/apis/clientset/versioned"
	"github.com/ovn-kubernetes/ovn-kubernetes/go-controller/pkg/util"
	"github.com/ovn-kubernetes/ovn-kubernetes/test/e2e/feature"
	"github.com/ovn-kubernetes/ovn-kubernetes/test/e2e/infraprovider"
	infraapi "github.com/ovn-kubernetes/ovn-kubernetes/test/e2e/infraprovider/api"
)

// On-prem clusters keep the API/Ingress virtual IPs inside the node subnet,
// where an unmanaged VTEP spanning that subnet also looks for node addresses.
// A VIP that is not keepalived-labelled (kube-vip, or any IPv6 VIP since
// labels do not exist there) must not be chosen over the node's own address.
// This emulates such a VIP with a plain host address numerically below the
// node IP, which is where VIPs conventionally sit, and forces a fresh VTEP IP
// selection with the VIP present.
//
// Serial: VTEP CIDRs must not overlap, so the shared node-subnet VTEP used by
// other EVPN tests is removed while this test runs its own VTEP over the same
// subnet. Tests that need the shared VTEP recreate it on demand (createVTEP is
// idempotent), so nothing is restored here.
var _ = ginkgo.Describe("EVPN: unmanaged VTEP IP selection", feature.RouteAdvertisements, feature.EVPN, ginkgo.Serial, func() {
	const fakeVIPIf = "lo"

	f := wrappedTestFramework("evpn-vtep-ip-selection")
	f.SkipNamespaceCreation = true

	var ictx infraapi.Context

	ginkgo.BeforeEach(func() {
		if !isLocalGWModeEnabled() {
			e2eskipper.Skipf("Test case only supported in Local Gateway mode")
		}
		ictx = infraprovider.Get().NewTestContext()
	})

	// nodeVTEPIPs returns the VTEP IPs annotated on the node for vtepName,
	// or nil when the node has no entry for it.
	nodeVTEPIPs := func(ctx context.Context, nodeName, vtepName string) ([]string, error) {
		n, err := f.ClientSet.CoreV1().Nodes().Get(ctx, nodeName, metav1.GetOptions{})
		if err != nil {
			return nil, fmt.Errorf("failed to get node %s: %w", nodeName, err)
		}
		vteps, err := util.ParseNodeVTEPs(n)
		if err != nil {
			if util.IsAnnotationNotSetError(err) {
				return nil, nil
			}
			return nil, fmt.Errorf("failed to parse VTEP annotation of node %s: %w", nodeName, err)
		}
		return vteps[vtepName].IPs, nil
	}

	ginkgo.It("prefers the node primary address over an unlabelled VIP in the node subnet", func() {
		nodeList, err := f.ClientSet.CoreV1().Nodes().List(context.Background(), metav1.ListOptions{})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to list nodes")
		gomega.Expect(nodeList.Items).NotTo(gomega.BeEmpty(), "cluster has no nodes")
		node := nodeList.Items[0]

		primary, err := util.GetNodeIfAddrAnnotation(&node)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to read primary interface address annotation of node %s", node.Name)
		gomega.Expect(primary.IPv4).NotTo(gomega.BeEmpty(), "test needs an IPv4 primary address on node %s", node.Name)
		primaryIP, primaryNet, err := net.ParseCIDR(primary.IPv4)
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to parse primary address %q of node %s", primary.IPv4, node.Name)

		// The fake VIP must sort before the node IP (so a lowest-address rule
		// would pick it) and must not be in use on the node subnet: skip the
		// network and gateway addresses and every node address.
		inUse := sets.New[string](primaryNet.IP.String(), incrementIP(primaryNet.IP, 1).String())
		for _, n := range nodeList.Items {
			for _, a := range n.Status.Addresses {
				inUse.Insert(a.Address)
			}
		}
		var fakeVIP net.IP
		for off := 2; off < 64; off++ {
			candidate := incrementIP(primaryNet.IP, off)
			if !primaryNet.Contains(candidate) || inUse.Has(candidate.String()) ||
				bytes.Compare(candidate.To16(), primaryIP.To16()) >= 0 {
				continue
			}
			fakeVIP = candidate
			break
		}
		if fakeVIP == nil {
			e2eskipper.Skipf("no free address below the node IP %s in %s to emulate a VIP", primaryIP, primaryNet)
		}

		ginkgo.By(fmt.Sprintf("Adding unlabelled VIP %s to node %s", fakeVIP, node.Name))
		_, err = infraprovider.Get().ExecK8NodeCommand(node.Name, []string{"ip", "addr", "add", fakeVIP.String() + "/32", "dev", fakeVIPIf})
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to add VIP %s to %s on node %s", fakeVIP, fakeVIPIf, node.Name)
		ictx.AddCleanUpFn(func() error {
			_, err := infraprovider.Get().ExecK8NodeCommand(node.Name, []string{"ip", "addr", "del", fakeVIP.String() + "/32", "dev", fakeVIPIf})
			if err != nil {
				return fmt.Errorf("failed to remove VIP %s from %s on node %s: %w", fakeVIP, fakeVIPIf, node.Name, err)
			}
			return nil
		})

		ginkgo.By("Waiting for the VIP to be known to the node address manager")
		gomega.Expect(wait.PollUntilContextTimeout(context.Background(), 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			n, err := f.ClientSet.CoreV1().Nodes().Get(ctx, node.Name, metav1.GetOptions{})
			if err != nil {
				return false, fmt.Errorf("failed to get node %s: %w", node.Name, err)
			}
			hostCIDRs, err := util.ParseNodeHostCIDRs(n)
			if err != nil {
				// annotation may not be updated yet; keep polling
				return false, nil
			}
			return hostCIDRs.Has(fakeVIP.String() + "/32"), nil
		})).To(gomega.Succeed(), "VIP %s did not appear in host-cidrs of node %s", fakeVIP, node.Name)

		// VTEP CIDRs must not overlap, so the shared node-subnet VTEP left by
		// a previous test has to go before this test's own VTEP over the same
		// subnet can be accepted. Waiting for the node to drop the shared
		// entry also guarantees the new VTEP gets a fresh IP selection.
		vtepClient, err := vtepclientset.NewForConfig(f.ClientConfig())
		gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to create VTEP client")
		ginkgo.By(fmt.Sprintf("Removing shared VTEP %s, which overlaps the node subnet", sharedNodeIPsVTEPName))
		err = vtepClient.K8sV1().VTEPs().Delete(context.Background(), sharedNodeIPsVTEPName, metav1.DeleteOptions{})
		if !apierrors.IsNotFound(err) {
			gomega.Expect(err).NotTo(gomega.HaveOccurred(), "failed to delete shared VTEP %s", sharedNodeIPsVTEPName)
		}
		gomega.Eventually(func() (bool, error) {
			_, err := vtepClient.K8sV1().VTEPs().Get(context.Background(), sharedNodeIPsVTEPName, metav1.GetOptions{})
			if apierrors.IsNotFound(err) {
				return true, nil
			}
			if err != nil {
				return false, fmt.Errorf("failed to get VTEP %s: %w", sharedNodeIPsVTEPName, err)
			}
			return false, nil
		}).WithTimeout(2*time.Minute).WithPolling(2*time.Second).Should(gomega.BeTrue(),
			"shared VTEP %s should be deleted", sharedNodeIPsVTEPName)
		gomega.Eventually(func() ([]string, error) {
			return nodeVTEPIPs(context.Background(), node.Name, sharedNodeIPsVTEPName)
		}).WithTimeout(2*time.Minute).WithPolling(2*time.Second).Should(gomega.BeEmpty(),
			"node %s should drop its %s VTEP annotation entry", node.Name, sharedNodeIPsVTEPName)

		// A test-owned name makes createVTEP register deletion on cleanup.
		vtepName := "evpn-vtep-ip-selection-" + rand.String(5)
		ginkgo.By(fmt.Sprintf("Creating unmanaged VTEP %s over the node subnet %s", vtepName, primaryNet))
		gomega.Expect(createVTEP(f, ictx, vtepName, []string{primaryNet.String()}, vtepv1.VTEPModeUnmanaged)).To(gomega.Succeed(),
			"failed to create VTEP %s", vtepName)
		gomega.Expect(waitForVTEPAccepted(f, vtepName)).To(gomega.Succeed(), "VTEP %s was not accepted", vtepName)

		ginkgo.By("Checking the node selected its primary address, not the VIP, as VTEP IP")
		gomega.Eventually(func() ([]string, error) {
			return nodeVTEPIPs(context.Background(), node.Name, vtepName)
		}).WithTimeout(60*time.Second).WithPolling(2*time.Second).Should(gomega.ConsistOf(primaryIP.String()),
			"VTEP IP of node %s must be the node primary address %s; %s is a VIP", node.Name, primaryIP, fakeVIP)
		framework.Logf("node %s selected %s as VTEP IP with VIP %s present", node.Name, primaryIP, fakeVIP)
	})
})
