#!/usr/bin/python

# -*- coding: utf-8 -*-

# Copyright (C) 2009-2012:
# Gabes Jean, naparuba@gmail.com
# Gerhard Lausser, Gerhard.Lausser@consol.de
# Gregory Starck, g.starck@gmail.com
# Hartmut Goebel, h.goebel@goebel-consult.de
#
# This file is part of Shinken.
#
# Shinken is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Shinken is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with Shinken. If not, see <http://www.gnu.org/licenses/>.

"""
This module job is to get configuration data from a mongodb database:
- get hosts
- get/set Web UI user's preferences
- get hosts availability logs
"""

# This module imports hosts and services configuration from a MySQL Database
# Queries for getting hosts and services are pulled from shinken-specific.cfg configuration file.
try:
    import uuid
except ImportError:
    uuid = None

import time
import re

from shinken.basemodule import BaseModule
from shinken.log import logger

try:
    import pymongo
    from pymongo import MongoClient
except ImportError:
    logger.error('[MongoDB] Can not import pymongo and/or MongoClient'
                 'Your pymongo lib is too old. '
                 'Please install it with a 3.x+ version from '
                 'https://pypi.python.org/pypi/pymongo')
    MongoClient = None

properties = {
    'daemons': ['arbiter', 'webui'],
    'type': 'mongodb',
    'external': False,
    'phases': ['configuration'],
}


# called by the plugin manager
def get_instance(plugin):
    logger.info("[MongoDB] Get MongoDB instance for plugin %s" % plugin.get_name())
    instance = Mongodb_generic(plugin)
    return instance


# Retrieve hosts from a Mongodb
class Mongodb_generic(BaseModule):
    def __init__(self, mod_conf):
        BaseModule.__init__(self, mod_conf)

        self.uri = getattr(mod_conf, 'uri', None)
        logger.info('[MongoDB] mongo uri: %s' % self.uri)
        self.replica_set = getattr(mod_conf, 'replica_set', None)
        if self.replica_set and int(pymongo.version[0]) < 3:
            logger.error('[MongoDB] Can not initialize module with '
                         'replica_set because your pymongo lib is too old. '
                         'Please install it with a 3.x+ version from '
                         'https://pypi.python.org/pypi/pymongo')
            return None
        self.database = getattr(mod_conf, 'database', 'shinken')
        logger.info('[MongoDB] database: %s' % self.database)
        
        self.hav_collection = getattr(mod_conf, 'hav_collection', 'availability')
        logger.info('[MongoDB] hosts availability collection: %s' % self.hav_collection)
        
        self.logs_collection = getattr(mod_conf, 'logs_collection', 'logs')
        logger.info('[MongoDB] Shinken logs collection: %s' % self.logs_collection)
        
        self.username = getattr(mod_conf, 'username', None)
        self.password = getattr(mod_conf, 'password', None)

        self.max_records = int(getattr(mod_conf, 'max_records', '200'))
        logger.info('[MongoDB] max records: %s' % self.max_records)

        self.con = None
        self.db = None

    # Called by Arbiter to say 'let's prepare yourself guy'
    def init(self):
        logger.info("[MongoDB] Try to open a Mongodb connection to %s, database: %s" % (self.uri, self.database))
        try:
            # BEGIN - Connection part
            if self.replica_set:
                self.con = MongoClient(self.uri, replicaSet=self.replica_set, fsync=False)
            else:
                self.con = MongoClient(self.uri, fsync=False)
            # END

            self.db = getattr(self.con, self.database)
            if self.username and self.password:
                self.db.authenticate(self.username, self.password)
        except Exception, e:
            logger.error("[MongoDB] Error %s:", e)
            raise
        logger.info("[MongoDB] Connection OK")

################################ Arbiter part #################################

    # Main function that is called in the CONFIGURATION phase
    def get_objects(self):
        if not self.db:
            logger.error("[MongoDB] Problem during init phase")
            return {}

        r = {}

        tables = ['hosts', 'services', 'contacts', 'commands', 'timeperiods']
        for t in tables:
            r[t] = []

            cur = getattr(self.db, t).find({'_state': {'$ne': 'disabled'}})
            for h in cur:
                #print "DBG: mongodb: get an ", t, h
                # We remove a mongodb specific property, the _id
                del h['_id']
                # And we add an imported_from property to say it came from
                # mongodb
                h['imported_from'] = 'mongodb:%s:%s' % (self.uri, self.database)
                r[t].append(h)

        return r


    def get_uniq_id(self, t, i):
        #by default we will return a very uniq id
        u = str(int(uuid.uuid4().int))
        
        is_tpl = (getattr(i, 'register', '1')  == '0')
        if is_tpl:
            return 'tpl-%s' % getattr(i, 'name', u)

        lst = {'hosts' : 'host_name', 'commands' : 'command_name',
               'timeperiods' : 'timeperiod_name',
               'contacts' : 'contact_name',
               }
        if t in lst:
            prop = lst[t]
            n = getattr(i, prop, None)
            if n:
                return n
            return u
        if t == 'services':
            return u

        print "Unknown TYPE in migration!"
        return u


    # Function called by the arbiter so we import the objects in our databases
    def import_objects(self, data):
        if not self.db:
            logger.error("[MongoDB]: error Problem during init phase")
            return False

        # Maybe we can't have a good way to have uniq id, if so, bail out
        if not uuid:
            logger.error("Your python version is too old. Please update to a 2.6 version to use this feature")
            return False


        for t in data:
            col = getattr(self.db, t)
            print "Saving objects %s" % t
            elts = data[t]
            for e in elts:
                print "Element", e
                e['_id'] = self.get_uniq_id(t, e)
                col.save(e)
            

        return True



#################################### WebUI parts ############################

    # We will get in the mongodb database the user preference entry, for the 'shinken-global' user
    # and get the key they are asking us
    def get_ui_common_preference(self, key):
        if not self.db:
            print "[MongoDB]: error Problem during init phase"
            return None

        e = self.db.ui_user_preferences.find_one({'_id': 'shinken-global'})

        print '[MongoDB] Get entry?', e
        # Maybe it's a new entryor missing this parameter, bail out
        if not e or not key in e:
            print '[MongoDB] no key or invalid one'
            return None

        return e.get(key)
        
    # We will get in the mongodb database the user preference entry, and get the key
    # they are asking us
    def get_ui_user_preference(self, user, key):
        if not self.db:
            print "[MongoDB]: error Problem during init phase"
            return None

        if not user:
            print '[MongoDB]: error get_ui_user_preference::no user'
            return None
        # user.get_name()
        e = self.db.ui_user_preferences.find_one({'_id': user.get_name()})

        print '[MongoDB] Get entry?', e
        # If no specific key is required, returns all user parameters ...
        if key is None:
            return e

        # Maybe it's a new entryor missing this parameter, bail out
        if not e or not key in e:
            print '[MongoDB] no key or invalid one'
            return None

        return e.get(key)

    # Same but for saving
    def set_ui_user_preference(self, user, key, value):
        if not self.db:
            print "[MongoDB]: error Problem during init phase"
            return None

        if not user:
            print '[MongoDB]: error get_ui_user_preference::no user'
            return None

        # Ok, go for update

        # check a collection exist for this user
        u = self.db.ui_user_preferences.find_one({'_id': user.get_name()})
        if not u:
            # no collection for this user? create a new one
            print "[MongoDB] No user entry for %s, I create a new one" % user.get_name()
            self.db.ui_user_preferences.save({'_id': user.get_name(), key: value})
        else:
            # found a collection for this user
            print "[MongoDB] user entry found for %s" % user.get_name()

        print '[MongoDB]: saving user pref', "'$set': { %s: %s }" % (key, value)
        r = self.db.ui_user_preferences.update({'_id': user.get_name()}, {'$set': {key: value}})
        print "[MongoDB] Return from update", r
        # Maybe there was no doc there, if so, create an empty one
        if not r:
            # Maybe the user exist, if so, get the whole user entry
            u = self.db.ui_user_preferences.find_one({'_id': user.get_name()})
            if not u:
                print "[MongoDB] No user entry for %s, I create a new one" % user.get_name()
                self.db.ui_user_preferences.save({'_id': user.get_name(), key: value})
            else:  # ok, it was just the key that was missing, just update it and save it
                u[key] = value
                print '[MongoDB] Just saving the new key in the user pref'
                self.db.ui_user_preferences.save(u)

    def set_ui_common_preference(self, key, value):
        if not self.db:
            print "[MongoDB]: error Problem during init phase"
            return None

        # check a collection exist for this user
        u = self.db.ui_user_preferences.find_one({'_id': 'shinken-global'})

        if not u:
            # no collection for this user? create a new one
            print "[MongoDB] No common entry, I create a new one"
            r = self.db.ui_user_preferences.save({'_id': 'shinken-global', key: value})
        else:
            # found a collection for this user
            print "[MongoDB] common entry found. Updating"
            r = self.db.ui_user_preferences.update({'_id': 'shinken-global'}, {'$set': {key: value}})

        if not r:
            print "[MongoDB]: error Problem during update/insert phase"
            return None



######################## WebUI availability part ############################

    # We will get in the mongodb database the host availability
    def get_ui_availability(self, name, range_start=None, range_end=None):
        if not self.db:
            logger.error("[MongoDB] error Problem during init phase, no database connection")
            return None

        logger.debug("[MongoDB] get_ui_availability, name: %s", name)
        hostname = None
        service = None
        if name is not None:
            hostname = name
            if '/' in name:
                service = name.split('/')[1]
                hostname = name.split('/')[0]
        logger.debug("[MongoDB] get_ui_availability, host/service: %s/%s", hostname, service)

        records=[]
        try:
            logger.debug("[MongoDB] Fetching records from database for host/service: '%s/%s'", hostname, service)

            query = []
            if hostname is not None:
                query.append( { "hostname" : { "$in": [ hostname ] }} )
            if service is not None:
                query.append( { "service" : { "$in": [ service ] }} )
            if range_start:
                query.append( { 'day_ts': { '$gte': range_start } } )
            if range_end:
                query.append( { 'day_ts': { '$lte': range_end } } )

            if len(query) > 0:
                logger.debug("[MongoDB] Fetching records from database with query: '%s'", query)

                for log in self.db[self.hav_collection].find({'$and': query}).sort([
                                    ("day",pymongo.DESCENDING), 
                                    ("hostname",pymongo.ASCENDING), 
                                    ("service",pymongo.ASCENDING)]).limit(self.max_records):
                    if '_id' in log:
                        del log['_id']
                    records.append(log)
            else:
                for log in self.db[self.hav_collection].find().sort([
                                    ("day",pymongo.DESCENDING), 
                                    ("hostname",pymongo.ASCENDING), 
                                    ("service",pymongo.ASCENDING)]).limit(self.max_records):
                    if '_id' in log:
                        del log['_id']
                    records.append(log)

            logger.debug("[MongoDB] %d records fetched from database.", len(records))
        except Exception, exp:
            logger.error("[MongoDB] Exception when querying database: %s", str(exp))

        return records



######################## WebUI availability part ############################

    # We will get in the mongodb database the logs
    def get_ui_logs(self, name, logs_type=None):
        if not self.db:
            logger.error("[MongoDB] error Problem during init phase, no database connection")
            return None

        logger.debug("[MongoDB] get_ui_logs, name: %s", name)
        hostname = None
        service = None
        if name is not None:
            hostname = name
            if '/' in name:
                service = name.split('/')[1]
                hostname = name.split('/')[0]
        logger.debug("[MongoDB] get_ui_logs, host/service: %s/%s", hostname, service)

        records=[]
        try:
            logger.debug("[MongoDB] Fetching records from database for host/service: '%s/%s'", hostname, service)

            query = []
            if hostname is not None:
                query.append( { "host_name" : { "$in": [ hostname ] }} )
            if service is not None:
                query.append( { "service_description" : { "$in": [ service ] }} )
            if logs_type and len(logs_type) > 0 and logs_type[0] != '':
                query.append({ "type" : { "$in": logs_type }})
            # if range_start:
                # query.append( { 'day_ts': { '$gte': range_start } } )
            # if range_end:
                # query.append( { 'day_ts': { '$lte': range_end } } )

            if len(query) > 0:
                logger.debug("[MongoDB] Fetching records from database with query: '%s'", query)

                for log in self.db[self.logs_collection].find({'$and': query}).sort([
                                    ("time",pymongo.DESCENDING)]).limit(self.max_records):
                    message = log['message']
                    m = re.search(r"\[(\d+)\] (.*)", message)
                    if m and m.group(2):
                        message = m.group(2)
                        
                    records.append({
                        "timestamp":    int(log["time"]),
                        "host":         log['host_name'],
                        "service":      log['service_description'],
                        "message":      message
                    })

            else:
                for log in self.db[self.logs_collection].find().sort([
                                    ("day",pymongo.DESCENDING)]).limit(self.max_records):
                    message = log['message']
                    m = re.search(r"\[(\d+)\] (.*)", message)
                    if m and m.group(2):
                        message = m.group(2)
                        
                    records.append({
                        "timestamp":    int(log["time"]),
                        "host":         log['host_name'],
                        "service":      log['service_description'],
                        "message":      message
                    })

            logger.debug("[MongoDB] %d records fetched from database.", len(records))
        except Exception, exp:
            logger.error("[MongoDB] Exception when querying database: %s", str(exp))

        return records
