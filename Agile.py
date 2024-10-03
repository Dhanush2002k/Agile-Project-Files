from elasticsearch import Elasticsearch
import pandas as pd

# Define the ElasticsearchManager class
class ElasticsearchManager:
    def __init__(self, host='http://localhost:9200'):
        self.es = Elasticsearch(hosts=[host])


    def createCollection(self, p_collection_name):
        if not self.es.indices.exists(index=p_collection_name):
            self.es.indices.create(index=p_collection_name)
            print(f"Collection '{p_collection_name}' created.")
        else:
            print(f"Collection '{p_collection_name}' already exists.")

    def indexData(self, p_collection_name, p_exclude_column):
        df = pd.read_csv('Employee Sample Data 1.csv')  # Ensure the CSV file is correctly referenced.
        # Exclude the specified column
        df = df.drop(columns=[p_exclude_column], errors='ignore')
        
        for _, row in df.iterrows():
            doc = row.to_dict()
            self.es.index(index=p_collection_name, document=doc)
        
        print(f"Data indexed into '{p_collection_name}', excluding column '{p_exclude_column}'.")

    def searchByColumn(self, p_collection_name, p_column_name, p_column_value):
        query = {
            "query": {
                "match": {
                    p_column_name: p_column_value
                }
            }
        }
        response = self.es.search(index=p_collection_name, body=query)
        return response['hits']['hits']

    def getEmpCount(self, p_collection_name):
        return self.es.count(index=p_collection_name)['count']

    def delEmpById(self, p_collection_name, p_employee_id):
        try:
            self.es.delete(index=p_collection_name, id=p_employee_id)
            print(f"Employee with ID '{p_employee_id}' deleted from '{p_collection_name}'.")
        except Exception as e:
            print(f"Error deleting employee: {e}")

    def getDepFacet(self, p_collection_name):
        query = {
            "size": 0,
            "aggs": {
                "department_counts": {
                    "terms": {
                        "field": "Department.keyword"
                    }
                }
            }
        }
        response = self.es.search(index=p_collection_name, body=query)
        return response['aggregations']['department_counts']['buckets']


# Main program execution
if __name__ == "__main__":
    # Replace with your name and last four digits of your phone number
    v_nameCollection = 'Hash_YourName'
    v_phoneCollection = 'Hash_YourPhoneLastFourDigits'

    es_manager = ElasticsearchManager()

    es_manager.createCollection(v_nameCollection)
    es_manager.createCollection(v_phoneCollection)

    emp_count = es_manager.getEmpCount(v_nameCollection)
    print(f"Employee count in '{v_nameCollection}': {emp_count}")

    es_manager.indexData(v_nameCollection, 'Department')
    es_manager.indexData(v_phoneCollection, 'Gender')

    es_manager.delEmpById(v_nameCollection, 'E02003')

    emp_count_after_deletion = es_manager.getEmpCount(v_nameCollection)
    print(f"Employee count in '{v_nameCollection}' after deletion: {emp_count_after_deletion}")

    it_employees = es_manager.searchByColumn(v_nameCollection, 'Department', 'IT')
    print(f"Employees in IT Department: {it_employees}")

    male_employees = es_manager.searchByColumn(v_nameCollection, 'Gender', 'Male')
    print(f"Male Employees: {male_employees}")

    it_employees_phone = es_manager.searchByColumn(v_phoneCollection, 'Department', 'IT')
    print(f"Employees in IT Department from phone collection: {it_employees_phone}")

    dep_facet = es_manager.getDepFacet(v_nameCollection)
    print(f"Department facet for '{v_nameCollection}': {dep_facet}")

    dep_facet_phone = es_manager.getDepFacet(v_phoneCollection)
    print(f"Department facet for '{v_phoneCollection}': {dep_facet_phone}")
