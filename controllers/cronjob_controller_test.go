/*


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controllers_test

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	kbatchv1 "k8s.io/api/batch/v1"
	kbatchv1beta1 "k8s.io/api/batch/v1beta1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	batchv1 "github.com/jace-ys/kubebuilder-tutorial/api/v1"
)

var _ = Describe("CronJob controller", func() {
	const (
		timeout  = time.Second * 10
		duration = time.Second * 10
		interval = time.Millisecond * 250
	)

	var (
		lookupKey types.NamespacedName
		cronJob   *batchv1.CronJob
	)

	ctx := context.Background()

	request := &batchv1.CronJob{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "batch.tutorial.kubebuilder.io/v1",
			Kind:       "CronJob",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      "test-cronjob",
			Namespace: "test-cronjob-namespace",
		},
		Spec: batchv1.CronJobSpec{
			Schedule: "1 * * * *",
			JobTemplate: kbatchv1beta1.JobTemplateSpec{
				Spec: kbatchv1.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							Containers: []corev1.Container{
								{
									Name:  "test-container",
									Image: "test-image",
								},
							},
							RestartPolicy: corev1.RestartPolicyOnFailure,
						},
					},
				},
			},
		},
	}

	BeforeEach(func() {
		lookupKey = types.NamespacedName{Name: request.Name, Namespace: request.Namespace}
		cronJob = new(batchv1.CronJob)
	})

	Context("the CronJob request", func() {
		It("should succeed", func() {
			Expect(k8sClient.Create(ctx, request)).Should(Succeed())
		})

		It("should create a CronJob with the desired spec", func() {
			Eventually(func() bool {
				err := k8sClient.Get(ctx, lookupKey, cronJob)
				if err != nil {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())

			Expect(cronJob.Spec.Schedule).Should(Equal(request.Spec.Schedule))
			Expect(cronJob.Spec.JobTemplate).Should(Equal(request.Spec.JobTemplate))
		})

		It("should create a CronJob with no active Jobs", func() {
			Consistently(func() (int, error) {
				err := k8sClient.Get(ctx, lookupKey, cronJob)
				if err != nil {
					return -1, err
				}
				return len(cronJob.Status.Active), nil
			}, duration, interval).Should(Equal(0))
		})
	})
})
