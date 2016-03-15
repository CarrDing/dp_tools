from tornado.web import RequestHandler
from models.check_conflict import Conflict

__all__ = (
    'ConflictListHandler',
)

class ConflictListHandler(RequestHandler):
    def get(self):
        conflict_state = Conflict().get()
        self.render('conflict.html', conflict_state=conflict_state)
