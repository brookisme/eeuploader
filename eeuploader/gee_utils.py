import ee
#
# GEE HELPERS
#
def task_info(task_id):
    """ print task info (copied from ee.cli)
    Args:
        - task_id<str|dict>:
            * <str> gee task id
            * <dict> must contain id=<TASK_ID> key-value pair
    """
    if isinstance(task_id,dict):
        task_id=task_id['id']
    for i, status in enumerate(ee.data.getTaskStatus(task_id)):
        if i:
            print()
        print('%s:' % status['id'])
        print('  State: %s' % status['state'])
        if status['state'] == 'UNKNOWN':
            continue
        print('  Type: %s' % TASK_TYPES.get(status.get('task_type'), 'Unknown'))
        print('  Description: %s' % status.get('description'))
        print('  Created: %s'
                    % _format_time(status['creation_timestamp_ms']))
        if 'start_timestamp_ms' in status:
            print('  Started: %s' % _format_time(status['start_timestamp_ms']))
        if 'update_timestamp_ms' in status:
            print('  Updated: %s'
                        % _format_time(status['update_timestamp_ms']))
        if 'error_message' in status:
            print('  Error: %s' % status['error_message'])
        if 'destination_uris' in status:
            print('  Destination URIs: %s' % ', '.join(status['destination_uris']))


def wait(task_id, timeout, noisy=True, raise_error=False):
    """ modified ee.cli.utils.wait_for_task 
        * silent mode
        * optional raise error
        * return final task status
    """
    start = time.time()
    elapsed = 0
    last_check = 0
    while True:
        elapsed = time.time() - start
        status = ee.data.getTaskStatus(task_id)[0]
        state = status['state']
        if state in TASK_FINISHED_STATES:
            error_message = status.get('error_message', None)
            if noisy: 
                print('Task %s ended at state: %s after %.2f seconds'
                        % (task_id, state, elapsed))
            if raise_error and error_message:
                raise ee.ee_exception.EEException('Error: %s' % error_message)
            return status
        remaining = timeout - elapsed
        if remaining > 0:
            time.sleep(min(10, remaining))
        else:
            break
    timeout_msg='Wait for task %s timed out after %.2f seconds' % (task_id, elapsed)
    status['TIMEOUT']=timeout_msg
    if noisy:
        print(timeout_msg)
    return status


@staticmethod
def assets(user,collection=None):
    """ list assets """
    pass

