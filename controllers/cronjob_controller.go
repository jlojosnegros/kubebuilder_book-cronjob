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
	"fmt"
	"sort"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/robfig/cron"
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

	// First clean up old Jobs, Do not want to leave too many lying around.
	// NB: deleting these is "best effort" -- if we fail on a particular one,
	// we won't requeue just to finish deleting.
	if cronJob.Spec.FailedJobHistoryLimit != nil {

		//sort failedJobs by StartTime
		sort.Slice(failedJobs, func(i, j int) bool {
			if failedJobs[i].Status.StartTime == nil {
				return failedJobs[j].Status.StartTime != nil
			}
			return failedJobs[i].Status.StartTime.Before(failedJobs[j].Status.StartTime)
		})

		for i, job := range failedJobs {
			// let alone the first ones in the list.
			if int32(i) >= int32(len(failedJobs))-*cronJob.Spec.FailedJobHistoryLimit {
				break
			}

			// once reached the limit delete  all the rest
			err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
			if client.IgnoreNotFound(err) != nil {
				log.Log.Error(err, "unable to delete old failed job", "job", job)
			} else {
				log.Log.V(0).Info("deleted old failed job", "job", job)
			}
		}
	}

	// repeat the same code but for successful jobs
	if cronJob.Spec.SucessfulJobHistoryLimit != nil {
		//sort successfulJobs by StartTime
		sort.Slice(successfulJobs, func(i, j int) bool {
			if successfulJobs[i].Status.StartTime == nil {
				return successfulJobs[j].Status.StartTime != nil
			}
			return successfulJobs[i].Status.StartTime.Before(successfulJobs[j].Status.StartTime)
		})

		for i, job := range successfulJobs {
			// let alone the first ones in the list.
			if int32(i) >= int32(len(successfulJobs))-*cronJob.Spec.SucessfulJobHistoryLimit {
				break
			}

			// once reached the limit delete  all the rest
			err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground))
			if client.IgnoreNotFound(err) != nil {
				log.Log.Error(err, "unable to delete old successful job", "job", job)
			} else {
				log.Log.V(0).Info("deleted old successful job", "job", job)
			}
		}
	}

	// 4. Check if we are suspended
	// If we are paused we do not want to run any more jobs, so lets stop here.
	if cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend {
		log.Log.V(1).Info("conjob suspended, skipping")
		return ctrl.Result{}, nil
	}

	// 5. Get next scheduled run

	//helpper to calculate next run and if we missed something.
	getNextSchedule := func(cronJob *batchv1.CronJob, now time.Time) (lastMissed time.Time, next time.Time, err error) {
		sched, err := cron.ParseStandard(cronJob.Spec.Schedule)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("Unparseable schedule %q: %v", cronJob.Spec.Schedule, err)
		}

		// for optimization purposes, cheat a bit and start from out last observed run time
		// we could reconstitue this here, but there is not much point.
		// since we have just updated it
		var earliestTime time.Time
		if cronJob.Status.LastScheduleTime != nil {
			earliestTime = cronJob.Status.LastScheduleTime.Time
		} else {
			earliestTime = cronJob.ObjectMeta.CreationTimestamp.Time
		}

		if cronJob.Spec.StartingDeadlineSeconds != nil {
			//controller is not going to schedule anything below this point
			schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))

			if schedulingDeadline.After(earliestTime) {
				earliestTime = schedulingDeadline
			}
		}

		if earliestTime.After(now) {
			return time.Time{}, sched.Next(now), nil
		}

		starts := 0
		for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
			lastMissed = t

			// An object might miss several starts. For example, if controller gets wedged on
			// Friday at 5:01 pm when everyone has gone home, and someone comes in on Tuesday AM
			// and discovers the problem and restarts the controller, then all the hourly jobs,
			// more than 80 of them for one hourly scheduledJob, should all start running
			// with no further intervention ( if scheduledJob allos concurrency and late starts)
			//
			// However, if there is a bug somewhere, or incorrect clock on controller's server or
			// apiservers ( for setting creationTimestamp), then there could be so many missed
			// start times ( it could be off by decades or more), that it would eat up all the
			// CPU and memory of this controller. In that case, we want to not try to list all
			// the missed start times.

			starts++
			if starts > 100 {
				// we can not get the most recent times so just return an empty slice
				return time.Time{}, time.Time{}, fmt.Errorf("Too many missed start times (> 100). Set or decrease .spec.startingDeadlineSeconds or check clock skew.")
			}
		}
		return lastMissed, sched.Next(now), nil
	}

	// figure out the next time we need to create a job at.
	missedRun, nextRun, err := getNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Log.Error(err, "unable to figure out CronJob schedule")

		// we do not really care about requeuing until we get an update that
		// fixes the schedule, so do not return an error
		return ctrl.Result{}, nil
	}

	// save the next time to reschedule to use it from now on.
	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(r.Now())}
	logger := log.Log.WithValues("now", r.Now(), "next run", nextRun)

	//6. Run a new Job if it is on schedule, not past the deadline, and not blocked by
	// our concurrency policy
	if missedRun.IsZero() {
		logger.V(1).Info("no upcoming scheduled times, sleeping until next")
		return scheduledResult, nil
	}

	//make sure we are not too late to start the run
	logger = logger.WithValues("current run", missedRun)
	tooLate := false
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		tooLate = missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds) * time.Second).Before(r.Now())
	}

	if tooLate {
		logger.V(1).Info("missed starting deadline for last run, sleeping till next")
		// TODO(directxman12): events
		return scheduledResult, nil
	}

	// once we know we can launch a new job, we must to know if the concurrency policy allow us to do it

	// figure out how to run this job -- concurrency policy might forbid us from running
	// multiple Jobs at the same time
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		logger.V(1).Info("concurrency policy blocks concurrent runs, skipping", "num active", len(activeJobs))
		return scheduledResult, nil
	}

	// ... or instruct us to replace existing ones ...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			// we do not care if the job was already deleted
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				logger.Error(err, "unable to delete active job", "job", activeJob)
				return ctrl.Result{}, err
			}
		}
	}

	// now that we already know what to do with the currently active jobs
	// we can create the new one

	constructJobForCronJob := func(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatch.Job, error) {
		// we want job names for a given nominal start time to have a
		// deterministic name to avoid the same job being created twice
		name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

		job := &kbatch.Job{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      make(map[string]string),
				Annotations: make(map[string]string),
				Name:        name,
				Namespace:   cronJob.Namespace,
			},
			Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
		}

		// Add Annotations
		for k, v := range cronJob.Spec.JobTemplate.Annotations {
			job.Annotations[k] = v
		}
		job.Annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)

		// Add labels
		for k, v := range cronJob.Spec.JobTemplate.Labels {
			job.Labels[k] = v
		}

		// Set controller reference
		err := ctrl.SetControllerReference(cronJob, job, r.Scheme)
		if err != nil {
			return nil, err
		}
		return job, err
	}

	// actually create the job
	job, err := constructJobForCronJob(&cronJob, missedRun)
	if err != nil {
		logger.Error(err, "unable to construct job from template")
		// do not bother requeuing until we get a change to the spec
		return scheduledResult, nil
	}

	// ... and create it on the cluster
	if err := r.Create(ctx, job); err != nil {
		logger.Error(err, "unable to create Job for CronJob", "job", job)
		return scheduledResult, err
	}

	logger.V(1).Info("created Job for CronJob run", "job", job)

	// we will requeue once we see the running job, and update our status
	return scheduledResult, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *CronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Complete(r)
}
