import pandas as pd
import sys

# methods


def load_input(filepath):
    headers = ['userid', 'expected-time-to-completion',
               'memory', 'priority', 'arrival-time']

    return pd.read_csv(filepath, skiprows=1, names=headers)


def write_output(output_df, filepath):
    output_df.to_csv(filepath, encoding='utf-8', index=False)


def init_constraints():
    for i in range(num_gpus):
        gpus[i] = {}
        gpus[i]['memory-taken'] = 0
        gpus[i]['completion-time'] = 0
        gpus[i]['number-of-current-jobs'] = 0


def init_simulation(input_df, output):
    jobs_df = input_df
    jobs_df['jobid'] = jobs_df.index
    # for job in jobs_df.iterrows():
    #     print(job[1]['arrival-time'])
    #     break
    
    # print('size: ' + str(len(jobs_df.index)))
    
    current_time = 0
    while (len(jobs_df.index) > 0):
        print(len(jobs_df))
        # filter jobs according to arrival time
        current_job_queue_df = jobs_df[jobs_df['arrival-time'] <= current_time]
        # print('current time: ' + str(current_time))
        # print('jobs left: ' + str(len(jobs_df.index)))
        # print('current_job_queue_df' + str(len(current_job_queue_df)))

        # find completed jobs and free up GPU
        if (len(output) > 0):
            for assigned_job in output.iterrows():
                if assigned_job[1]['completion-time'] == current_time:
                    # add memory back to gpu
                    gpus[assigned_job[1]['gpu-assigned']
                            ]['number-of-current-jobs'] -= 1
                    gpus[assigned_job[1]['gpu-assigned']
                            ]['memory-taken'] -= assigned_job[1]['memory']
                            
        if len(current_job_queue_df) > 0:
            # assign jobs
            for job in current_job_queue_df.iterrows():
                shortest_completion_time = sys.maxsize
                gpu_to_assign = -1

                # print('******************************* job id: ', str(job[1]['jobid']), '***************************************')
                # print('expected-time-to-completion: ', job[1]['expected-time-to-completion'])

                for gpu_id in range(num_gpus):
                    if gpus[gpu_id]['memory-taken'] + job[1]['memory'] <= gpu_fixed_memory:
                        if gpus[gpu_id]['number-of-current-jobs'] == 0:
                            gpu_to_assign = gpu_id
                            shortest_completion_time = current_time + job[1]['expected-time-to-completion']
                        else:
                            # calculate completion time
                            consecutive_completion_time = calculate_consecutive_completion_time(
                                current_time, gpus[gpu_id], job)
                            concurrent_completion_time = calculate_concurrent_completion_time(
                                output, current_time, gpu_id, gpus[gpu_id], job)
                            # print('consecutive ' + str(consecutive_completion_time))
                            # print('concurrent ' + str(concurrent_completion_time))

                            # if gpu_id == 0 and concurrent_completion_time <= consecutive_completion_time:
                            #     shortest_completion_time = concurrent_completion_time
                            #     gpu_to_assign = gpu_id
                            # elif gpu_id == 0:
                            #     shortest_completion_time = consecutive_completion_time

                            # assign shortest_completion_time and gpu_to_assign if shorter
                            if concurrent_completion_time <= consecutive_completion_time and concurrent_completion_time < shortest_completion_time:
                                shortest_completion_time = concurrent_completion_time
                                gpu_to_assign = gpu_id
                                # print('assigned to ', gpu_to_assign)
                            
                            # print('shortest_completion_time: ', str(shortest_completion_time))
                            # remove current assigned gpu if there exist a gpu where there is a shorter completion time if ran in series
                            # assign gpu id when consecutive > concurrent time
                            # elif consecutive_completion_time < shortest_completion_time and gpu_to_assign != -1:
                            #     shortest_completion_time = consecutive_completion_time
                            #     gpu_to_assign = -1
                # print('gpu to assign: ' + str(gpu_to_assign))
                
                if gpu_to_assign != -1:
                    # deduct memory from gpu
                    gpus[gpu_to_assign]['memory-taken'] += job[1]['memory']

                    # series to append
                    append_series = pd.Series([job[1]['userid'], job[1]['expected-time-to-completion'], job[1]['memory'], job[1]['priority'],
                                               job[1]['arrival-time'], job[1]['jobid'], gpu_to_assign, current_time, shortest_completion_time], index=output.columns)
                    # print(append_series)

                    output = output.append(append_series, ignore_index=True)

                    # update num jobs in gpu
                    gpus[gpu_to_assign]['number-of-current-jobs'] += 1
                    
                    # update completion time for all affected jobs
                    for job in output.iterrows():
                        # is it > ? if end right now, will not affect right
                        # print(job[1])
                        if job[1]['gpu-assigned'] == gpu_to_assign and job[1]['completion-time'] > current_time:
                            job[1]['completion-time'] = recalculate_completion_time(gpus[gpu_to_assign]['number-of-current-jobs'], current_time, job[1]['completion-time'])
                    # update unassigned jobs left
                    # jobs at current time snapshot
                    current_job_queue_df = current_job_queue_df[current_job_queue_df['jobid'] != job[1]['jobid']]
                    # current_job_queue_df.drop(current_job_queue_df.index[job[1]['jobid']])
                    # print('job id: ' + str(job[1]['jobid']))
                    jobs_df = jobs_df[jobs_df['jobid'] != job[1]['jobid']]
                    # jobs_df = jobs_df.drop([job[1]['jobid']])
                    # total_size = len(jobs_df.index)  # remaining jobs in total
                    
        current_time += 1
    return output


def calculate_consecutive_completion_time(current_time, gpu_details, job):
    completed_time = 0
    if current_time < gpu_details['completion-time']:
        completed_time = gpu_details['completion-time'] + \
            job[1]['expected-time-to-completion']
    else:
        completed_time = current_time + job[1]['expected-time-to-completion']
    return completed_time


def calculate_concurrent_completion_time(ongoing_jobs, current_time, gpu_id, gpu_details, job):
    penalty = {1: 1, 2: 2, 3: 4, 4: 8, 5: 16}
    k = penalty[gpu_details['number-of-current-jobs'] + 1]

    # print('current time: ')
    # print(current_time)
    # return
    # for now base this off the current job
    job_completion_time = current_time + job[1]['expected-time-to-completion']
    job_not_completed = True

    # get all jobs in current gpu which are not completed
    ongoing_jobs_in_current_gpu = ongoing_jobs[ongoing_jobs['gpu-assigned'] == gpu_id]
    ongoing_jobs_in_current_gpu = ongoing_jobs_in_current_gpu[ongoing_jobs_in_current_gpu['completion-time'] >= current_time]
    
    num_of_jobs_removed = 0
    while job_not_completed:

        shortest_job_end_time = 0
        first_time = True
        if len(ongoing_jobs_in_current_gpu) > 0:
            # print('entered ongoing jobs if bracket')
            for curr_gpu_job in ongoing_jobs_in_current_gpu.iterrows():
                if first_time:
                    first_time = False
                    shortest_job_end_time = curr_gpu_job[1]['completion-time']
                # gets the next shortest completion time
                elif curr_gpu_job[1]['completion-time'] < shortest_job_end_time and curr_gpu_job[1]['completion-time'] > current_time:
                    shortest_job_end_time = curr_gpu_job[1]['completion-time']

            # calculate the number of job that would be completed from the current time to the shortest job end time
            for curr_gpu_job in ongoing_jobs_in_current_gpu.iterrows():
                if curr_gpu_job[1]['completion-time'] == shortest_job_end_time:
                    num_of_jobs_removed += 1

            # get the time left for the next shortest completion time
            time_left_for_curr_k_value = shortest_job_end_time - current_time + 1

            # check if time left for the shortest completion time is longer than the time left to complete the current job and if there are any other jobs left
            if job_completion_time < (current_time + shortest_job_end_time) or num_of_jobs_removed == gpu_details['number-of-current-jobs']:
                time_left_for_curr_k_value = job_completion_time - current_time
                shortest_job_end_time = job_completion_time

            # print('num jobs in gpu at current time')
            # print(gpu_details['number-of-current-jobs'] +
            #       1 - num_of_jobs_removed)
            # print(current_time)
            # get the current k value
            k = penalty[gpu_details['number-of-current-jobs'] + 1 - num_of_jobs_removed]

            # calculate the additional time taken for the current job
            job_completion_time = job_completion_time - \
                time_left_for_curr_k_value + time_left_for_curr_k_value * k

            current_time = shortest_job_end_time

            # condition for stopping the while loop
            if job_completion_time <= current_time:
                job_not_completed = False

        else:
            # print('entered else')
            job_not_completed = False
    # print('concurrent test:')
    # print(job_completion_time)
    return job_completion_time

def recalculate_completion_time(num_jobs_in_gpu, current_time, current_completion_time):
    penalty = {1: 1, 2: 2, 3: 4, 4: 8, 5: 16}
    k = penalty[num_jobs_in_gpu]
    time_left = current_completion_time - current_time
    return current_time + time_left * k



# containers
output = pd.DataFrame(columns=['userid', 'expected-time-to-completion',
                               'memory', 'priority', 'arrival-time', 'jobid', 'gpu-assigned', 'assignment-time', 'completion-time'])  # result - to be written into csv
gpus = {}

# inputs
input_df = load_input('./input/single.csv')
num_gpus = 5
gpu_fixed_memory = 1000

# main
init_constraints()
output = init_simulation(input_df, output)

# output to csv
write_output(output, './output/output.csv')
