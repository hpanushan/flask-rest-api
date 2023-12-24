from waitress import serve
from flask import Flask, render_template, jsonify
from flask_restx import Api, Resource, fields, reqparse
from get_post_cors import get_post_cors
from mysql_db import MySQL_DB
import os,logging

app = Flask(__name__)
api = Api(app, version='1.0V', title='Movies API', description='Movies API')

# Logging format
logging.basicConfig(filename='log/app_Log',level=logging.INFO, format='%(asctime)s [%(levelname)s] %(filename)s %(lineno)d %(message)s')

# Namespaces
moviens = api.namespace('api/v1/movie', description='movies operations')

# movies model
movie = api.model('movie', {
    'id': fields.Integer(readonly=True, description='The task unique identifier'),
    'name': fields.String(required=True, description='The movie title',default="movie title"),
    'genre': fields.String(required=True, description='The movie genre',default="movie genre"),
    'year': fields.Integer(required=True, description='The movie year',default=2000),
})

# read environment variables
DB_HOST = os.environ['DB_HOST']
DB_USER = os.environ['DB_USER']
DB_PASSWORD = os.environ['DB_PASSWORD']
DB_NAME = os.environ['DB_NAME']
DB_TABLE = os.environ['DB_TABLE']

@moviens.route('')
class Get_All_Movies(Resource):

    def get(self):
        try:
            # Instantiate DB instance
            db_obj = MySQL_DB(DB_HOST,DB_USER,DB_PASSWORD,DB_NAME)
            records = db_obj.get_all_items(DB_TABLE)
            db_obj.close_connection()
           
            return records, 200    # No need to jsonify here
        except Exception:
            return get_post_cors({'result':'internal server error'},500)

    @api.expect(movie) 
    def post(self):
        try:
            data = api.payload          # Get data sent through POST (Dictionary data type)
            # Values
            name,genre,year = data['name'],data['genre'],data['year']

            # Instantiate DB instance
            db_obj = MySQL_DB(DB_HOST,DB_USER,DB_PASSWORD,DB_NAME)
            db_obj.add_record(DB_TABLE,name,genre,year)
            db_obj.close_connection()

            return get_post_cors({'result':"added new movie"},201)
    
        except Exception:
            return get_post_cors({'result':'internal server error'},500)
    
@moviens.route('/<int:id>')
class Update_Delete_Movies_By_Id(Resource):
    @api.expect(movie) 
    def put(self,id):
        try:
            # Instantiate DB instance
            db_obj = MySQL_DB(DB_HOST,DB_USER,DB_PASSWORD,DB_NAME)
            if db_obj.check_record_exists(DB_TABLE,id):
                data = api.payload          # Get data sent through POST (Dictionary data type)
                # Values
                name,genre,year = data['name'],data['genre'],data['year']
                db_obj.update_record(DB_TABLE,name,genre,year)
                db_obj.close_connection()
                return get_post_cors({'result':"updated movie"},201)
            else:
                return get_post_cors({'result':'record not found'},404)
    
        except Exception:
            return get_post_cors({'result':'internal server error'},500)
        
    def delete(self,id):
        try:
            # Instantiate DB instance
            db_obj = MySQL_DB(DB_HOST,DB_USER,DB_PASSWORD,DB_NAME)

            if db_obj.check_record_exists(DB_TABLE,id):
                db_obj.delete_record(DB_TABLE,id)
                db_obj.close_connection()
                return get_post_cors({'result':"deleted movie"},201)
            else:
                return get_post_cors({'result':'record not found'},404)
    
        except Exception:
            return get_post_cors({'result':'internal server error'},500)

@moviens.route('/get-by-genre/<string:genre>')
class Get_Movies_By_Genre(Resource):
    def get(self,genre):
        try:
            # Instantiate DB instance
            db_obj = MySQL_DB(DB_HOST,DB_USER,DB_PASSWORD,DB_NAME)
            records = db_obj.get_items_by_genre(DB_TABLE,genre)
            db_obj.close_connection()
           
            return records, 200    # No need to jsonify here
        except Exception:
            return get_post_cors({'result':'internal server error'},500)
        
@moviens.route('/get-record-count')
class Get_Record_Count(Resource):
    def get(self):
        try:
            # Instantiate DB instance
            db_obj = MySQL_DB(DB_HOST,DB_USER,DB_PASSWORD,DB_NAME)
            num_records = db_obj.get_record_count(DB_TABLE)
            db_obj.close_connection()
           
            return {"count": num_records}, 200    # No need to jsonify here
        except Exception:
            return get_post_cors({'result':'internal server error'},500)

# main driver function
if __name__ == '__main__':
 
    # serve(app,host="0.0.0.0",port="5000")
    app.run(port="5000",debug=True)

