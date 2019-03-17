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
        gpus[i]['memory-left'] = gpu_fixed_memory
        gpus[i]['completion-time'] = 0
        gpus[i]['number-of-current-jobs'] = 0


def init_simulation(input_df):
    jobs_df = input_df
    jobs_df['jobid'] = jobs_df.index

    while (jobs_df.size > 0):
        current_time = 0
        # filter jobs according to arrival time
        current_job_queue_df = jobs_df[jobs_df['arrival-time'] <= current_time]

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
                        gpu_details, job)

                    # assign shortest_completion_time and gpu_to_assign if shorter
                    if concurrent_completion_time < consecutive_completion_time and concurrent_completion_time < shortest_completion_time:
                        shortest_completion_time = concurrent_completion_time
                        gpu_to_assign = gpu_id

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


def calculate_concurrent_completion_time(gpu_details, job):
    penalty = {1: 1, 2: 2, 3: 4, 4: 8, 5: 16}
    k = penalty[gpu_details['number-of-current-jobs'] + 1]

    # edit here

    return 1


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
