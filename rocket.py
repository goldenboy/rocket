# Copyright 2010 Kaspars Dancis
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.



import logging, base64, os

from google.appengine.api import datastore, datastore_types, datastore_errors

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

from rocket.common import *
import json, yaml



SQLITE_INTEGER = "integer"
SQLITE_REAL = "real"
SQLITE_TEXT = "text"
SQLITE_BLOB = "blob"



class Rocket(webapp.RequestHandler):
    
    def get_config(self):
        if not hasattr(self, "config"):
            rocket_yaml = os.path.join(os.path.dirname(os.path.abspath(os.path.dirname(__file__))), "rocket.yaml")
            self.config = yaml.load(file(rocket_yaml, "r"))
            self.query_filter = None
            if self.config.has_key("query_filter"):                
                query_filter_name = self.config["query_filter"] 
                try:
                    i = query_filter_name.rfind('.')
                    if i <= 0:
                        raise Exception("Config error: query_filter - no module specified")
                                                    
                    p = query_filter_name[:i]
                    m = query_filter_name[i+1:]
                    
                    exec "from %s import %s as query_filter" % (p, m) in locals()
                    
                    self.query_filter = query_filter
                except Exception, e:
                    raise Exception("Config error: cannot import query_filter - %s" % e.message)
            else:
                raise Exception("Config error: query_filter must be specified")    
            
        return self.config
    
    
    
    def unauthorized(self, error = None):
        self.error(403)
        if error:
            logging.error(u"Unauthorized: %s" % error)
            self.response.out.write(u'<error>Unauthorized: %s</error>\n' % error)
        else:
            logging.error(u"Unauthorized")
    
    
    
    def bad_request(self, error):    
        self.error(400)
        logging.error(u"Bad Request: %s" % error)
        self.response.out.write(u'<error>%s</error>\n' % error)



    def not_found(self, error):    
        self.error(404)
        logging.error(u"Not Found: %s" % error)
        self.response.out.write(u'<error>%s</error>\n' % error)
        
        
        
    def server_error(self, error, exception=None):
        self.error(500)
        
        if exception != None:            
            logging.exception(u"Server Error: %s" % error)            
            self.response.out.write(u'<error>Server Error: %s\n%s</error>\n' % (error, exception))
        else:
            logging.error(u"Server Error: %s" % error)
            self.response.out.write(u'<error>Server Error: %s</error>\n' % error)            
        
        
        
    def get(self):    
        path = self.request.path.split("/")
                
        self.response.headers['Content-Type'] = 'application/json'
            
        if len(path) < 3 or path[2] == '': 
            return self.bad_request("Please specify an entity kind")
        
        kind = path[2]
    
        updates = []
        
        query = datastore.Query(kind)
        
        timestamp_field = self.request.get("timestamp")       
        batch_size = int(self.request.get("count"))
            
        f = self.request.get("from") 
        if f: 
            query['%s >' % timestamp_field] = from_iso(f)
            
        self.get_config() # to ensure query_filter is imported
        self.query_filter(self.request, kind, query)
    
        query.Order(timestamp_field)
            
        entities = query.Get(batch_size, 0)
        
        for entity in entities:
            update = {
                "key": {
                    "type": get_sqllite_type(entity.key()),
                    "value": get_sqllite_value(entity.key()),
                }
            }
            
            for field, value in entity.items():                        
                update[field] = {
                    "type": get_sqllite_type(value),
                    "value": get_sqllite_value(value), 
                }                
                        
            updates.append(update)
                        
        res = json.dumps({"updates": updates})
        
        self.response.out.write(res)
        
        
        
        
    def post(self):
        path = self.request.path.split("/")
        
        self.response.headers['Content-Type'] = 'text/plain'

        if len(path) < 3 or path[2] == '': 
            return self.bad_request(u'Please specify an entity kind\n')
        
        kind = path[2]
        
        entity = None
        clear_cache = False
        
        key_name_or_id = self.request.get(TYPE_KEY)
        if key_name_or_id:
            if key_name_or_id[0] in "0123456789":
                key = datastore.Key.from_path(kind, int(key_name_or_id)) # KEY ID
            else:
                key = datastore.Key.from_path(kind, key_name_or_id) # KEY NAME
                
            try: entity = datastore.Get(key)
            except datastore_errors.EntityNotFoundError: pass
            
        if not entity:
            if key_name_or_id:
                
                if key_name_or_id[0] in "0123456789":
                    return self.not_found(u'Entity with AppEngine ID=%s is not found.\n' % key_name_or_id)
                    
                entity = datastore.Entity(kind=kind,name=key_name_or_id)
            else:
                entity = datastore.Entity(kind=kind)
        else:
            clear_cache = True
                
        args = self.request.arguments()
        for arg in args:
            if arg != TYPE_KEY:                
                bar = arg.find('|')
                if bar > 0:
                    field_type = arg[:bar]
                    field_name = arg[bar + 1:]
                    value = self.request.get(arg)                    
                    if field_type.startswith("*"):
                        field_type = field_type[1:]
                        if len(value) == 0:
                            if entity.has_key(field_name):
                                del entity[field_name]
                        else:
                            entity[field_name] = map(lambda v: rocket_to_ae(field_type, v), value.split('|'))
                    else:
                        entity[field_name] = rocket_to_ae(field_type, value)
                            
        datastore.Put(entity)
        
        after_send = self.request.get("after_send")
        if after_send:
            try:
                i = after_send.rfind('.')
                if i <= 0:
                    raise Exception("No module specified")                                
                p = after_send[:i]
                m = after_send[i+1:]
                exec "from %s import %s as after_send_method" % (p, m) in locals()
                exec "after_send_method(entity)" in locals()
            except Exception, e:                
                return self.server_error("Error invoking AFTER_SEND event handler (%s)" % after_send,e)
        
        self.response.out.write(u'<ok/>')



def get_sqllite_type(value):
    if isinstance(value, bool) or isinstance(value, long) or isinstance(value, int):
        return SQLITE_INTEGER
    elif isinstance(value, float):
        return SQLITE_REAL
    elif isinstance(value, datastore_types.Blob):
        return SQLITE_BLOB
    else:
        return SQLITE_TEXT



def get_sqllite_value(value):
    # DATETIME
    if isinstance(value, datetime):
        return to_iso(value)
    
    # BOOL
    elif isinstance(value, bool):
        return int("%d" % value)
    
    # KEY
    elif isinstance(value, datastore_types.Key):
        return value.id_or_name()
    
    # LIST
    if isinstance(value, list):
        return "|".join(value)
    
    # KEY
    elif isinstance(value, datastore_types.Blob):
        return base64.b64encode(value)
    
    # ALL OTHERS
    else:
        return value



def rocket_to_ae(field_type, rocket_value):
    if not rocket_value:
        ae_value = None
    elif field_type == TYPE_DATETIME or field_type == TYPE_TIMESTAMP:
        ae_value = from_iso(rocket_value)
    elif field_type == TYPE_BOOL:
        ae_value = bool(int(rocket_value))
    elif field_type == TYPE_LONG:
        ae_value = long(rocket_value)
    elif field_type == TYPE_FLOAT:
        ae_value = float(rocket_value)
    elif field_type == TYPE_INT:
        ae_value = int(rocket_value)
    elif field_type == TYPE_TEXT:
        ae_value = datastore_types.Text(rocket_value.replace('&#124;','|'))
    elif field_type == TYPE_REFERENCE:
        slash = rocket_value.find("/")
        if slash > 0:
            kind = rocket_value[:slash]
            key_name_or_id = rocket_value[slash + 1:]        
            if key_name_or_id[0] in "0123456789":
                key_name_or_id = int(key_name_or_id)
            ae_value = datastore.Key.from_path(kind, key_name_or_id)  
        else:
            logging.error("invalid reference value: %s" % rocket_value)
            ae_value = None
    elif field_type == TYPE_BLOB:
        ae_value = datastore_types.Blob(base64.b64decode(rocket_value))
    else: #str
        ae_value = (u"%s" % rocket_value).replace('&#124;','|')
    
    return ae_value
                                                                
             

application = webapp.WSGIApplication([('/rocket/.*', Rocket)], debug=True)

def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()