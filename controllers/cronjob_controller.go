/*
Copyright 2021.

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

package controllers

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	batchv1 "tutorial.kubebuilder.io/cronjob/api/v1"
)

// CronJobReconciler reconciles a CronJob object
type CronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/status,verbs=get;update;patch
//+kubebuilder:rbac:groups=batch.tutorial.kubebuilder.io,resources=cronjobs/finalizers,verbs=update
//+kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
//+kubebuilder:rbac:groups=batch,resources=jobs/status,verbs=get

var (
	scheduledTimeAnnotation = "batch.tutorial.kubebuilder.io/scheduled-at"
)

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the CronJob object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.10.0/pkg/reconcile
// Requeue when we either see a running job (done automatically) or it's time for the next scheduled run.
//
// The basic logic of our CronJob controller is this:
// 1. Load the named CronJob
// 2. List all active jobs, and update the status
// 3. Clean up old jobs according to the history limits
// 4. Check if we're suspended (and don't do anything else if we are)
// 5. Get the next scheduled run
// 6. Run a new job if it;s on schedule, not past the deadline, and not blocked by our concurrency policy
func (r *CronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	_ = log.FromContext(ctx)

	// 1. Load the named CronJob
	// We'll fetch the CronJob using our client.
	// All client methods take a context (to allow for cancellation) as their first argument, and the object in question as their last.
	// Get is a bit special, in that it takes a NamespacedName as the middle argument (most don't have a middle argument, as we'll see below).
	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		log.Log.Error(err, "Unable to fetch CronJob")
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	// 2. List all active jobs, and update the status
	// To fully update our status we need to list all child jobs in this namespace that belong to this CronJob.
	// We use List for that.
	// We use variadic options to set:
	// - the namespace
	// - a field match : which is actually an index lookup that we set up bellow
	// NOTE:
	// 		The reconciler fetches all jobs owned by the cronjob for the status.
	//		As our number of cronjobs increases, looking these up can become quite slow as we have to filter through all of them.
	//		For a more efficient lookup, these jobs will be indexed locally on the controller's name.
	//		A jobOwnerKey field is added to the cached job objects.
	//		This key references the owning controller and functions as the index.
	//		Later in this document we will configure the manager to actually index this field.
	var childJobs kbatch.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Log.Error(err, "Unable to list child Jobs")
		return ctrl.Result{}, err
	}

	// Now that we have all the jobs we (this CronJob) owns it is time to classify them
	// into active, successful and failed keeping track of the most recent
	// All this in order to reconstruct status
	// WARN:
	// 		You should **NEVER** read Status from the main Object ( the root object of the controller)
	// 		You should be able to reconstruct status on every run.

	// find the active list of jobs
	var activeJobs []*kbatch.Job
	var successfulJobs []*kbatch.Job
	var failedJobs []*kbatch.Job
	var mostRecentTime *time.Time

	// We can check if a job is "finished" using Conditions.
	// We consider a job "finished" if it has a "Complete" or "Failed" condition marked as `true`
	// NOTE:
	//		Status Conditions allow us to add extensible status information to our objects
	//		that other humans and controllers can examine to check things like completion or health
	isJobFinished := func(job *kbatch.Job) (bool, kbatch.JobConditionType) {
		for _, c := range job.Status.Conditions {
			if (c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed) && c.Status == corev1.ConditionTrue {
				return true, c.Type
			}
		}
		return false, ""
	}

	// we use a helpper to extract the scheduled time from the annotation that we added during job creation.
	// jlom: que no tengo ni puta idea de donde se añadió

	getScheduledTimeForJob := func(job *kbatch.Job) (*time.Time, error) {
		timeRaw := job.Annotations[scheduledTimeAnnotation]
		if len(timeRaw) == 0 {
			return nil, nil // ? no error?
		}

		timeParsed, err := time.Parse(time.RFC3339, timeRaw)
		if err != nil {
			return nil, err
		}

		return &timeParsed, nil
	}

	for i, job := range childJobs.Items {
		_, finishedType := isJobFinished(&job)
		switch finishedType {
		case "": //ongoing
			activeJobs = append(activeJobs, &childJobs.Items[i])
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, &childJobs.Items[i])
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, &childJobs.Items[i])
		}

		// we will store the launch time in an annotation,
		// so we will reconstitute that from the active jobs themselves
		scheduledTimeForJob, err := getScheduledTimeForJob(&job)
		if err != nil {
			log.Log.Error(err, "Unable to parse schedule time for child job", "job", &job)
			continue
		}

		if scheduledTimeForJob != nil {
			if mostRecentTime == nil { // no other time yet
				mostRecentTime = scheduledTimeForJob
			} else if mostRecentTime.Before(*scheduledTimeForJob) {
				mostRecentTime = scheduledTimeForJob
			}
		}
	}

	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}

	//Rebuilding active list. First reset
	cronJob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := reference.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Log.Error(err, "Unable to make a reference to active job", "job", activeJob)
			continue
		}

		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	log.Log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	// Now using al the gathered data  we update the Status of the CRD
	if err := r.Status().Update(ctx, &cronJob); err != nil {
		log.Log.Error(err, "unable to update CronJob status")
		return ctrl.Result{}, err
	}

	// Now that we have updated Status we can focus on ensuring  that status of the world matched Spec

	//3: Clean up old jobs according to the history limit

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Complete(r)
}
