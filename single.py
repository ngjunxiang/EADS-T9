import pandas as pd
import sys
import time
import math

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
    
    # print('size: ' + str(len(jobs_df.index)))
    
    current_time = 0
    start_time = time.time()
    while (len(jobs_df.index) > 0):
        # print(len(jobs_df))
        # filter jobs according to arrival time

        current_job_queue_df = jobs_df[jobs_df['arrival-time'] <= current_time]
        current_job_queue_df = current_job_queue_df.sort_values(by=['expected-time-to-completion'])

        num_priority3 = current_job_queue_df[current_job_queue_df['priority'] == 3]
        contains_all_priority_3 = False

        if num_priority3.shape[0] == current_job_queue_df.shape[0]:
            contains_all_priority_3 = True
        # print('job_df: ', jobs_df.shape[0])
        # print('current time: ' + str(current_time))
        # print('jobs left: ' + str(len(jobs_df.index)))
        # print('******************************************* Next Batch ***************************************************')
        print('current_job_queue_df: ' + str(current_job_queue_df.shape[0]))
        # start_time = time.time()
        if len(current_job_queue_df) > 0:
            # assign jobs
            for job in current_job_queue_df.iterrows():
                shortest_completion_time = sys.maxsize
                gpu_to_assign = -1
                selected_gpu_changed_times = []
                priority = job[1]['priority']
                # print('priority: ', str(priority))
                # print('******************************* job id: ', str(job[1]['jobid']), '***************************************')
                # print('expected-time-to-completion: ', job[1]['expected-time-to-completion'])
                # start_time2 = time.time()
                if priority != 3 or contains_all_priority_3:
                    for gpu_id in range(num_gpus):
                        # print('************************************* gpu ' + str(gpu_id) + ' **************************************************************')
                        ongoing_jobs_in_current_gpu = gpus[gpu_id]['number-of-current-jobs']
                        # print('num_jobs_in_gpu: ', ongoing_jobs_in_current_gpu)
                        if gpus[gpu_id]['memory-taken'] + job[1]['memory'] <= gpu_fixed_memory and ongoing_jobs_in_current_gpu < 2:
                            if gpus[gpu_id]['number-of-current-jobs'] == 0:
                                gpu_to_assign = gpu_id
                                shortest_completion_time = current_time + job[1]['expected-time-to-completion']
                                selected_gpu_changed_times = []
                                break
                            # else:
                            #     # calculate completion time
                            #     consecutive_completion_time = calculate_consecutive_completion_time(
                            #         output, current_time, gpu_id, gpus[gpu_id], job)
                                # new_completion_times, concurrent_completion_time = calculate_concurrent_completion_time(
                                #     output, current_time, gpu_id, gpus[gpu_id], job)
                                # print('consecutive ' + str(consecutive_completion_time))
                                # print('concurrent ' + str(concurrent_completion_time))

                                # assign shortest_completion_time and gpu_to_assign if shorter
                                # if concurrent_completion_time <= consecutive_completion_time and concurrent_completion_time < shortest_completion_time:
                                #     shortest_completion_time = concurrent_completion_time
                                #     gpu_to_assign = gpu_id
                                #     selected_gpu_changed_times = new_completion_times
                                # if consecutive_completion_time < shortest_completion_time:
                                #     shortest_completion_time = consecutive_completion_time
                                #     gpu_to_assign = gpu_id
                                    # print('assigned to ', gpu_to_assign)
                else:
                    for gpu_id in range(2):
                        ongoing_jobs_in_current_gpu = gpus[gpu_id]['number-of-current-jobs']
                        if gpus[gpu_id]['memory-taken'] + job[1]['memory'] <= gpu_fixed_memory and ongoing_jobs_in_current_gpu < 2:
                            if gpus[gpu_id]['number-of-current-jobs'] == 0:
                                gpu_to_assign = gpu_id
                                shortest_completion_time = current_time + job[1]['expected-time-to-completion']
                                selected_gpu_changed_times = []
                                break
                            # else:
                            #     # calculate completion time
                            #     consecutive_completion_time = calculate_consecutive_completion_time(
                            #         output, current_time, gpu_id, gpus[gpu_id], job)
                                # new_completion_times, concurrent_completion_time = calculate_concurrent_completion_time(
                                #     output, current_time, gpu_id, gpus[gpu_id], job)
                                # print('consecutive ' + str(consecutive_completion_time))
                                # print('concurrent ' + str(concurrent_completion_time))

                                # assign shortest_completion_time and gpu_to_assign if shorter
                                # if concurrent_completion_time <= consecutive_completion_time and concurrent_completion_time < shortest_completion_time:
                                #     shortest_completion_time = concurrent_completion_time
                                #     gpu_to_assign = gpu_id
                                #     selected_gpu_changed_times = new_completion_times
                                # if concurrent_completion_time < shortest_completion_time:
                                #     shortest_completion_time = consecutive_completion_time
                                #     gpu_to_assign = gpu_id
                                    # selected_gpu_changed_times = new_completion_times
                # # print('gpu to assign: ' + str(gpu_to_assign))
                # elapsed_time = time.time() - start_time2
                # print('elapsed time 2: ', elapsed_time)
            # allocate 1 gpu for long jobs
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
                    # print('selected_gpu_changed_times: ', selected_gpu_changed_times)
                    # update completion time for all affected jobs
                    for jobid, new_completion_time in selected_gpu_changed_times:
                        index = output.index[output['jobid'] == jobid]
                        output.at[index,'completion-time'] = new_completion_time
                    # # update unassigned jobs left
                    # jobs at current time snapshot
                    # print(output)
                    current_job_queue_df = current_job_queue_df[current_job_queue_df['jobid'] != job[1]['jobid']]
                    # print('job id: ' + str(job[1]['jobid']))
                    jobs_df = jobs_df[jobs_df['jobid'] != job[1]['jobid']]
                    # find completed jobs and free up GPU
            # elapsed_time = time.time() - start_time
            # print('total elapsed time: ', elapsed_time)
                # elapsed_time = time.time() - start_time
                # print('elapsed time 3: ', elapsed_time)
        all_completed_job_at_current_time = output[output['completion-time'] == current_time]
        # previous_time = current_time - 5
        # if previous_time < 0:
        #     previous_time = 0
        # all_completed_job_at_current_time = all_completed_job_at_current_time[all_completed_job_at_current_time['completion-time'] >= previous_time]
        if (all_completed_job_at_current_time.shape[0] > 0):
            # print('all_completed_job_at_current_time: ', all_completed_job_at_current_time.shape[0])
            for assigned_job in all_completed_job_at_current_time.iterrows():
                # if assigned_job[1]['completion-time'] == current_time:
                    # add memory back to gpu
                gpus[assigned_job[1]['gpu-assigned']
                        ]['number-of-current-jobs'] -= 1
                gpus[assigned_job[1]['gpu-assigned']
                        ]['memory-taken'] -= assigned_job[1]['memory']
        current_time += 1
        # print('output: ')
        # print(output)
        print('Current Time: ' + str(current_time))
    elapsed_time = time.time() - start_time
    print('elapsed time: ', elapsed_time)
    return output


def calculate_consecutive_completion_time(ongoing_jobs, current_time,gpu_id, gpu_details, job):
    completed_time = 0
    ongoing_jobs_in_current_gpu = ongoing_jobs[ongoing_jobs['gpu-assigned'] == gpu_id]
    ongoing_jobs_in_current_gpu = ongoing_jobs_in_current_gpu[ongoing_jobs_in_current_gpu['completion-time'] >= current_time]
    latest_completion_time = ongoing_jobs_in_current_gpu['completion-time'].max()
    # print('latest_completion_time: ', latest_completion_time)
    if current_time < latest_completion_time:
        completed_time = latest_completion_time + \
            job[1]['expected-time-to-completion']
    else:
        completed_time = current_time + job[1]['expected-time-to-completion']
    return completed_time


def calculate_concurrent_completion_time(ongoing_jobs, current_time, gpu_id, gpu_details, job):
    penalty = {1: 1, 2: 2, 3: 4, 4: 8, 5: 16}
    # k = penalty[gpu_details['number-of-current-jobs'] + 1]
    # print('gpu id: ' + str(gpu_id))
    new_completion_times = []
    # print('current time: ')
    # print(current_time)
    # return
    # for now base this off the current job
    job_completion_time = current_time + job[1]['expected-time-to-completion']
    job_not_completed = True

    # get all jobs in current gpu which are not completed
    ongoing_jobs_in_current_gpu = ongoing_jobs[ongoing_jobs['gpu-assigned'] == gpu_id]
    ongoing_jobs_in_current_gpu = ongoing_jobs_in_current_gpu[ongoing_jobs_in_current_gpu['completion-time'] >= current_time]
    ongoing_jobs_in_current_gpu = ongoing_jobs_in_current_gpu.sort_values(by=['completion-time'])
    
    # if ongoing_jobs_in_current_gpu.shape[0] == 5:
    #     return new_completion_times, 99999999999999999999999999999999999999
    # print(ongoing_jobs_in_current_gpu)
    # print('gpu id: ' + str(gpu_id))
    # print(ongoing_jobs_in_current_gpu.shape[0])
    # start_time = time.time()
    while job_not_completed:
        # print('curr time before: ', str(current_time))
        shortest_job_end_time = 0

        # start_time = time.time()
        if len(ongoing_jobs_in_current_gpu) > 0:
            # get the current shortest job in the gpu out
            curr_job_in_gpu = ongoing_jobs_in_current_gpu.iloc[0]
            shortest_job_end_time = curr_job_in_gpu['completion-time']
            
            # get the time left for the next shortest completion time
            time_left_for_curr_k_value = shortest_job_end_time - current_time + 1

            leftover_time = 0
            # print('job_completion_time: ', job_completion_time)
            # print('shortest_job_end_time: ', shortest_job_end_time)
            # check if time left for the shortest completion time is longer than the time left to complete the current job and if there are any other jobs left
            if job_completion_time < shortest_job_end_time:
                time_left_for_curr_k_value = job_completion_time - current_time + 1
                leftover_time = shortest_job_end_time - job_completion_time + leftover_time
                shortest_job_end_time = job_completion_time
            
            # print('leftover_time: ', leftover_time)
            # print('num jobs in gpu at current time')
            # print(gpu_details['number-of-current-jobs'] +
            #       1 - num_of_jobs_removed)
            # print(current_time)
            # k = 0
            # previous_k_penalty = 0
            num_jobs_in_gpu = ongoing_jobs_in_current_gpu.shape[0]
            # if num_jobs_in_gpu >= 5:
            #     num_jobs_in_gpu = 5
            #     k = penalty[num_jobs_in_gpu]
            #     previous_k_penalty = penalty[num_jobs_in_gpu]
            # else:
            # get the current k value
            k = penalty[num_jobs_in_gpu + 1]
            previous_k_penalty = penalty[num_jobs_in_gpu]

            # print('Penalty: ' + str(k))
            # print('Previous penalty: ' + str(previous_k_penalty))

            indexes = list(ongoing_jobs_in_current_gpu.index.values)
            # print(ongoing_jobs_in_current_gpu)
            # print('memory: ', str(curr_job_in_gpu['memory']))

            # calculate the time period affected for all jobs with the new k value
            new_additional_time = math.ceil(time_left_for_curr_k_value / previous_k_penalty * k + (job[1]['memory']))

            # new end time for current job
            new_shortest_job_end_time = shortest_job_end_time - time_left_for_curr_k_value + new_additional_time + leftover_time

            # print('new_shortest_job_end_time: ' + str(new_shortest_job_end_time))
            # adding ended job with new end times to a list
            # new_completion_times.append((curr_job_in_gpu, new_shortest_job_end_time))
            # print('length of dataframe: ', len(indexes))
            num_dropped_indexes = 0
            # print(ongoing_jobs_in_current_gpu)
            # adding new times for all gpu and removing the shortest job/jobs with the same end time as the current shortest jobs
            for index in range(len(indexes)):
                # print(index)
                curr_time_taken = ongoing_jobs_in_current_gpu['completion-time'].iloc[index - num_dropped_indexes]
                # print(curr_time_taken)
                new_time_taken = curr_time_taken - time_left_for_curr_k_value + new_additional_time + 1
                ongoing_jobs_in_current_gpu.at[indexes[index],'completion-time'] = new_time_taken
                # print(new_time_taken)
                # print(new_shortest_job_end_time)

                if new_time_taken == new_shortest_job_end_time:
                    new_completion_times.append((ongoing_jobs_in_current_gpu.iloc[index - num_dropped_indexes]['jobid'],new_shortest_job_end_time))
                    # print('ongoing_jobs_in_current_gpu: ', ongoing_jobs_in_current_gpu)
                    ongoing_jobs_in_current_gpu = ongoing_jobs_in_current_gpu.drop(indexes[index])
                    num_dropped_indexes = num_dropped_indexes + 1
                    # print('ongoing_jobs_in_current_gpu after: ', ongoing_jobs_in_current_gpu)

            # calculate the additional time taken for the current job
            job_completion_time = job_completion_time - time_left_for_curr_k_value + new_additional_time

            current_time = new_shortest_job_end_time
            # print('curr time after: ', str(current_time))
            # condition for stopping the while loop
            if job_completion_time <= current_time:
                job_not_completed = False

        else:
            # print('entered else')
            job_not_completed = False
    # print('new completion times: ', new_completion_times)
    # print('concurrent test:')
    # print(job_completion_time)
    # elapsed_time = time.time() - start_time
    # print('elapsed time: ', elapsed_time)
    return new_completion_times, job_completion_time

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
input_df = load_input('./competition-2019/mixed.csv')
num_gpus = 5
gpu_fixed_memory = 1000

# main
init_constraints()
output = init_simulation(input_df, output)

# output to csv
write_output(output, './output/output.csv')
