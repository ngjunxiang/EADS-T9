import pandas as pd

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
        gpus[i]['memory-left'] = gpu_fixed_memory
        gpus[i]['completion-time'] = 0
        gpus[i]['number-of-current-jobs'] = 0


def init_simulation(input_df):
    jobs_df = input_df
    jobs_df['jobid'] = jobs_df.index

    # for job in jobs_df.iterrows():
    #     print(job[1]['arrival-time'])
    #     break

    current_time = 0
    while (jobs_df.size > 0):
        # filter jobs according to arrival time
        current_job_queue_df = [job for job in jobs_df.iterrows() if job[1]['arrival-time'] <= current_time]
        # print(current_time)
        print(current_job_queue_df)

        if len(current_job_queue_df) != 0:

            # assign jobs
            for index, job in current_job_queue_df:
                shortest_completion_time = 0
                gpu_to_assign = -1


                for gpu_id, gpu_details in gpus.items():
                    if not gpu_details['memory-left'] + gpu_details['memory'] > gpu_fixed_memory:
                        # calculate completion time
                        consecutive_completion_time = calculate_consecutive_completion_time(
                            gpu_details, job)
                        concurrent_completion_time = calculate_concurrent_completion_time(
                            output, current_time, gpu_id, gpu_details, job)

                        # assign shortest_completion_time and gpu_to_assign if shorter
                        if concurrent_completion_time < consecutive_completion_time and concurrent_completion_time < shortest_completion_time:
                            shortest_completion_time = concurrent_completion_time
                            gpu_to_assign = gpu_id
                        # remove current assigned gpu if there exist a gpu where there is a shorter completion time if ran in series
                        elif consecutive_completion_time < shortest_completion_time and gpu_to_assign != -1:
                            shortest_completion_time = consecutive_completion_time
                            gpu_to_assign = -1

                if gpu_to_assign != -1:
                    # deduct memory from gpu
                    gpus[gpu_to_assign]['memory'] -= job['memory']
                    output.append(job + [job['jobid'], gpu_to_assign,
                                        current_time, current_time + shortest_completion_time])

                    # update completion time for all affected jobs
                    for index, job in output:
                        # is it > ? if end right now, will not affect right
                        if job['assigned-gpu'] == gpu_to_assign and job['completion-time'] >= current_time:
                            # not sure???
                            job['completion-time'] = job['assigned-time'] + shortest_completion_time

                    # update unassigned jobs left
                    # jobs at current time snapshot
                    current_job_queue_df.drop(job['jobid'])
                    jobs_df.drop(job['jobid'])  # remaining jobs in total

            # find completed jobs
            for index, assigned_job in output:
                if assigned_job['assignment-time'] + assigned_job['completion-time'] == current_time:
                    # add memory back to gpu
                    gpus[assigned_job['assigned-gpu']
                        ]['memory'] += assigned_job['memory']

        current_time += 1


def calculate_consecutive_completion_time(gpu_details, job):
    return gpu_details['completion-time'] + job['expected-time-to-completion']


def calculate_concurrent_completion_time(ongoing_jobs, current_time, gpu_id, gpu_details, job):
    penalty = {1: 1, 2: 2, 3: 4, 4: 8, 5: 16}
    k = penalty[gpu_details['number-of-current-jobs'] + 1]

    # for now base this off the current job
    job_completion_time = current_time + job['expected-time-to-completion']
    job_not_completed = True

    # get all jobs in current gpu which are not completed
    ongoing_jobs_in_current_gpu = pd.DataFrame(columns=['userid', 'expected-time-to-completion',
                                                        'memory', 'priority', 'arrival-time', 'jobid', 'gpu-assigned', 'assignment-time', 'completion-time'])
    
    for index, curr_job in ongoing_jobs:
        # checks if job is still in the current gpu at this current time
        if curr_job['gpu-assigned'] == gpu_id and curr_job['completion-time'] >= current_time:
            ongoing_jobs_in_current_gpu.append(curr_job)
    
    num_of_jobs_removed = 0
    while job_not_completed:
        
        next_shortest_job = ''
        shortest_job_end_time = 0
        for index, curr_gpu_job in ongoing_jobs_in_current_gpu:
            if index == 0:
                next_shortest_job = curr_gpu_job
                shortest_job_end_time = curr_gpu_job['completion-time']
            # gets the next shortest completion time
            elif curr_gpu_job['completion-time'] < shortest_job_end_time and curr_gpu_job['completion-time'] > current_time:
                next_shortest_job = curr_gpu_job
                shortest_job_end_time = curr_gpu_job['completion-time']
        
        # calculate the number of job that would be completed from the current time to the shortest job end time
        for index, curr_gpu_job in ongoing_jobs_in_current_gpu:
            if curr_gpu_job['completion-time'] == shortest_job_end_time:
                num_of_jobs_removed += 1

        # get the time left for the next shortest completion time
        time_left_for_curr_k_value = shortest_job_end_time - current_time + 1

        # check if time left for the shortest completion time is longer than the time left to complete the current job and if there are any other jobs left
        if job_completion_time < (current_time + shortest_job_end_time) or num_of_jobs_removed == gpu_details['number-of-current-jobs']:
            time_left_for_curr_k_value = job_completion_time - current_time
            shortest_job_end_time = job_completion_time
        
        # get the current k value
        k = penalty[gpu_details['number-of-current-jobs'] + 1 - num_of_jobs_removed]

        # calculate the additional time taken for the current job
        job_completion_time = job_completion_time - time_left_for_curr_k_value + time_left_for_curr_k_value * k

        current_time = shortest_job_end_time

        # condition for stopping the while loop
        if job_completion_time <= current_time:
            job_not_completed = False

    return job_completion_time


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
init_simulation(input_df)

# output to csv
write_output(output, './output/output.csv')
