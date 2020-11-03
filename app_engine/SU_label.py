#!/usr/bin/env python
# coding: utf-8

# Copyright 2016 Google Inc.
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

# [START imports]
import os
import urllib

from google.appengine.api import users
from google.appengine.ext import ndb

import jinja2
import webapp2

JINJA_ENVIRONMENT = jinja2.Environment(
    loader=jinja2.FileSystemLoader(os.path.dirname(__file__)),
    extensions=['jinja2.ext.autoescape'],
    autoescape=True)
# [END imports]
# We set a parent key on the 'Greetings' to ensure that they are all
# in the same entity group. Queries across the single entity group
# will be consistent. However, the write rate should be limited to
# ~1/second.
class SU_challenge1000_(ndb.Model):
    maybe_agriculture = ndb.BooleanProperty()
    key_main = ndb.IntegerProperty()
    label_agriculture = ndb.StringProperty()
    plateforme = ndb.StringProperty()
    producteur = ndb.StringProperty()    
    nom_struc = ndb.StringProperty()
    prez_struc = ndb.StringProperty()
    prez_produit_struc = ndb.StringProperty()


# [START main_page]
class MainPage(webapp2.RequestHandler):

    def get(self):
        query = SU_challenge1000_.query()
        
        #data = [dict(e.items()) for e in query.fetch(10)]#[0]
        #print(data)
        #template_values = data

        template_values = {
            'nom_struc': "Yam agro INDUSTRIE",
            'prez_struc': u"Nous avons deux types de sirop a savoir le sirop de bissap et de gingembre. Un biscuit les cookies au chocolat et une confiture celle à la mangue. Nous envisageons mettre sur le marché d'ici mars 2020 trois nouveaux sirops et une confiture",
            'prez_produit_struc': u"Entreprise spécialisée dans la transformation agroalimentaire made in Burkina Faso,  nous proposons aux consommateurs burkinabé des sirop , des biscuits et de la confiture."
        }

        template = JINJA_ENVIRONMENT.get_template('index.html')
        self.response.write(template.render(template_values))
# [END main_page]



# [START app]
app = webapp2.WSGIApplication([
    ('/', MainPage)
], debug=True)
# [END app]
