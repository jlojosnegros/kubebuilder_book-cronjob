package controllers

import (
	"context"
	"reflect"
	"time"

	. "github.com/onsi/ginko"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	cronjobv1 "tutorial.kubebuilder.io/project/api/v1"
)

var _ = Describe("CronJob controller", func() {

	// Define utility constants for object names and testing timeouts/durations
	// and intervals
	const (
		CronJobName      = "test-cronjob"
		CronJobNamespace = "default"
		JobName          = "test-job"

		timeout  = time.Second * 10
		duration = time.Second * 10
		interval = time.Millisecond * 250
	)

	Context("When updating CronJob Status", func() {
		It("Should increase CronJob Status.Active count when new Jobs are created", func() {
			By("By creating a new CronJob")
			ctx := context.Background()

			cronJob := &cronjobv1.CronJob{
				TypeMeta: metav1.TypeMeta{
					APIVersion: "batch.tutorial.kubebuilder.io/v1",
					Kind:       "CronJob",
				},
				ObjectMeta: metav1.ObjectMeta{
					Name:      CronJobName,
					Namespace: CronJobNamespace,
				},
				Spec: cronjobv1.CronJobSpec{
					Schedule: "1 * * * *",
					JobTemplate: batchv1.JobSpec{
						// For simplicity we only fill out the required fields.
						Template: v1.PodTemplateSpec{
							Spec: v1.PodSpec{
								//For simplicity we only fill out the requiered fields.
								Containers: []v1.Container{
									{
										Name:  "test-container",
										Image: "test-image",
									},
								},
								RestartPolicy: v1.RestartPolicyOnFailure,
							},
						},
					},
				},
			}
			Expect(k8sClient.Create(ctx, cronJob)).Should(Succeed())

			cronJobLookupKey := types.NamespacedName{
				Name:      CronJobName,
				Namespace: CronJobNamespace,
			}
			createdCronJob := &cronjobv1.CronJob{}

			// We will need to retry getting this newly created CronJob,
			// given that creation may not inmediately happen
			Eventually(func() bool {
				err := k8sClient.Get(ctx, cronJobLookupKey, createdCronJob)
				if err != nil {
					return false
				}
				return true
			}, timeout, interval).Should(BeTrue())

			// Let's make sure our Schedule string value was properly converted/handled.
			Expect(createdCronJob.Spec.Schedule).Should(Equal("1 * * * *"))

			By("By checking the CronJob has zero active Jobs")
			Consistently(func() (int, error) {
				err := k8sClient.Get(ctx, cronJobLookupKey, createdCronJob)
				if err != nil {
					return -1, err
				}
				return len(createdCronJob.Status.Active), nil
			}, duration, interval).Should(Equal(0))

			By("Creating a new Job")

			testJob := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      JobName,
					Namespace: CronJobNamespace,
				},
				Spec: batchv1.JobSpec{
					Template: v1.PodTemplateSpec{
						Spec: v1.PodSpec{
							Containers: []v1.Container{
								{
									Name:  "test-container",
									Image: "test-image",
								},
							},
							RestartPolicy: v1.RestartPolicyOnFailure,
						},
					},
				},
				// Set "Active: 2" in status to simulate it is running in 2 pods
				Status: batchv1.JobStatus{
					Active: 2,
				},
			}

			// Note: that your CronJob's GroupVersionKind is required to set up this
			// owner reference
			kind := reflect.TypeOf(cronjobv1.CronJob{}).Name()
			gvk := cronjobv1.GroupVersion.WithKind(kind)

			controllerRef := metav1.NewControllerRef(createdCronJob, gvk)
			testJob.SetOwnerReferences([]metav1.OwnerReference{*controllerRef})
			Expect(k8sClient.Create(ctx, testJob)).Should(Succeed())


			By("By checking that the CronJob has one active job")
			Eventually(func () ([]string, error) {
				err := k8sClient.Get(ctx, cronJobLookupKey, createdCronJob)
				if err != nil {
					return nil, err
				}

				names := []string{}
				for _, job := range createdCronJob.Status.Active {
					names = append(names, job.Name)
				}
				return names, nil
			}, timeout, interval).Should(ConsistOf(JobName))
		})
	})
})
