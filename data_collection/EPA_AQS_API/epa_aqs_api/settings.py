from environs import Env

env = Env()
env.read_env()

EMAIL = env.str("email")
API_KEY = env.str("api_key")