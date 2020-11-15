#from server import server

from flask import Flask

def init_app():

    app = Flask(
        __name__,
        instance_relative_config=False
    )

    #app.config.from_object(config,Config)

    with app.app_context():
        from . import routes

        import crtdb
        app = crtdb(app)

if __name__ == "__main__":
    app.run()