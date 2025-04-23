import os


# function resolves environment variables: both direct variable and file assignments
# this provides a means of accommodating 'secrets'
# this module is inspired by the current direction recommended in:
# https://docs.docker.com/engine/swarm/secrets/#build-support-for-docker-secrets-into-your-images
def get_env(env_key):
    env_value = str()
    if env_key in os.environ:
        env_value = os.getenv(env_key)

    # resolve for file assignment
    file_env = os.environ.get(env_key + "_FILE", str())
    if len(file_env) > 0:
        try:
            with open(file_env, 'r') as mysecret:
                data = mysecret.read().replace('\n', '')
                env_value = data
        except:
            pass

    return env_value
