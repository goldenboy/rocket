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



import logging, base64, os, datetime, time

from google.appengine.api import datastore, datastore_types, datastore_errors

from google.appengine.ext import webapp
from google.appengine.ext.webapp.util import run_wsgi_app

from django.utils import simplejson as json
import yaml



DATETIME = "datetime"
TIMESTAMP = "timestamp"
BOOL = "bool"
LONG = "long"
FLOAT = "float"
INT = "int"
TEXT = "text"
KEY = "key"
LIST = "list"
STR = "str"
BLOB = "blob"

KEY = "key"
TIMESTAMP = "timestamp"
IS_DIRTY = "is_dirty"



class Rocket(webapp.RequestHandler):
    
    def get_config(self):
        if not hasattr(self, "config"):
            rocket_yaml = os.path.join(os.path.dirname(os.path.dirname(__file__)), "rocket.yaml")
            self.config = yaml.load(file(rocket_yaml, "r"))
            
            def import_filter(type):
                if self.config.has_key(type):                
                    query_filter_name = self.config[type] 
                    try:
                        i = query_filter_name.rfind('.')
                        if i <= 0:
                            raise Exception("Config error: %s - no module specified" % type)
                                                        
                        p = query_filter_name[:i]
                        m = query_filter_name[i+1:]
                        
                        exec "from %s import %s as filter" % (p, m) in locals()
                        
                        return filter
                    except Exception, e:
                        raise Exception("Config error: cannot import %s - %s" % (type, e.message))
                else:
                    raise Exception("Config error: %s must be specified" % type)    
            
            self.query_filter = import_filter("query_filter")
            self.update_filter = import_filter("update_filter")            
            
        return self.config
    
    
    
    def bad_request(self, error):    
        self.error(400)
        logging.error(u"Bad Request: %s" % error)
        self.response.out.write(json.dumps({"error": error}))

   
        
    def post(self):
        
        # PARSE CONTEXT AND PARAMETERS
         
        self.response.headers['Content-Type'] = 'application/json'

        path = self.request.path.split("/")        
        if len(path) < 3 or path[2] == '': 
            return self.bad_request(u'Please specify an entity kind\n')
                
        kind = path[2]
        
        # PROCESS INBOUND UPDATES
        
        keys_in = set()
        updates_in = json.loads(self.request.get("updates"))
        
        entity_config = self.get_config()["entities"][kind]
        
        for update in updates_in:
            key_name_or_id = update[KEY]
            
            keys_in.add(key_name_or_id)
            
            if key_name_or_id[0] in "0123456789":
                key = datastore.Key.from_path(kind, int(key_name_or_id)) # KEY ID
                is_id = True
            else:                
                key = datastore.Key.from_path(kind, key_name_or_id) # KEY NAME
                is_id = False
            
            try: 
                entity = datastore.Get(key)

                if not self.update_filter(self.request, kind, entity):
                    logging.error("update on existing entity is filter, key: %s" % key_name_or_id)
                    continue
                    
            except datastore_errors.EntityNotFoundError:
                if is_id:
                    entity = datastore.Entity(kind=kind,id=int(key_name_or_id))
                else:
                    entity = datastore.Entity(kind=kind,name=key_name_or_id)
                
            for attr_name in update:
                if attr_name != KEY and attr_name != TIMESTAMP and attr_name != IS_DIRTY:
                    if entity_config.has_key(attr_name):
                        attr_config = entity_config[attr_name]
                    else:
                        attr_config = None
                        
                    if attr_name == TIMESTAMP:
                        value = datetime.datetime.now()
                    else:
                        value = update[attr_name] 
                        
                    entity[attr_name] = js_to_appengine_value(value, attr_config)

            # update timestamp to current time so that it get's picked up by other clients
            entity[TIMESTAMP] = datetime.datetime.now()                                    

            if not self.update_filter(self.request, kind, entity):
                logging.error("updated entity is filtered, key: %s" % key_name_or_id)
                continue
                    
            datastore.Put(entity)
            
            
        # PROCESS OUTBOUND UPDATES            

        batch_size = int(self.request.get("count"))

        updates_out = []
        
        query = datastore.Query(kind)
            
        f = self.request.get("from") 
        if f: 
            query['%s >= ' % TIMESTAMP] = datetime_from_iso(f)
            
        self.query_filter(self.request, kind, query)
    
        query.Order(TIMESTAMP)
        
        batch_start_timestamp = appengine_to_js_value(datetime.datetime.now())
            
        entities = query.Get(batch_size, 0)
        
        for entity in entities:
            key = appengine_to_js_value(entity.key())
            
            # do not send back updates that we just received from the same client
            if not key in keys_in:                                
                update = {
                    KEY: key
                }
                
                for field, value in entity.items():                        
                    update[field] = appengine_to_js_value(value)
                            
                updates_out.append(update)
                        
        res = json.dumps({
            "updates": updates_out, 
            "timestamp": batch_start_timestamp,
        })
        
        self.response.out.write(res)




def appengine_to_js_value(value):
    # DATETIME
    if isinstance(value, datetime.datetime):
        return datetime_to_iso(value)
    
    # BOOL
    elif isinstance(value, bool):
        return int("%d" % value)
    
    # KEY
    elif isinstance(value, datastore_types.Key):
        return value.id_or_name()
    
    # LIST
    if isinstance(value, list):
        return map(lambda value: appengine_to_js_value(value), list)
    
    # KEY
    elif isinstance(value, datastore_types.Blob):
        return base64.b64encode(value)
    
    # ALL OTHERS
    else:
        return value



def js_to_appengine_value(value, attr_config):
    if not value or not attr_config:
        return None
    
    type = attr_config['type']
    if type == DATETIME:
        return datetime_from_iso(value)
        
    elif type == INT:
        return int(value)
    
    elif type == LONG:
        return long(value)
    
    elif type == BOOL:
        return bool(value)
        
    elif type == TEXT:
        return datastore_types.Text(value)
        
    elif type == KEY:
        kind = attr_config['kind']
        if value[0] in "0123456789":
            return datastore.Key.from_path(kind, int(value))
        else:            
            return datastore.Key.from_path(kind, value)
          
    elif type == BLOB:
        return datastore_types.Blob(base64.b64decode(value))

    elif type == LIST:
        return map(lambda value: js_to_appengine_value(value, attr_config['items']), value.split("|"))
        
    else: #str
        return value
    
    
    
def datetime_from_iso(s):
    dt = datetime.datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S")
    try: dt = dt.replace(microsecond = int(s[20:]))
    except: pass
    return dt



def datetime_to_iso(dt):
    return dt.isoformat()    
                                                                
             

application = webapp.WSGIApplication([('/rocket/.*', Rocket)], debug=True)

def main():
  run_wsgi_app(application)

if __name__ == "__main__":
  main()