from bson import ObjectId
import pandas as pd
import re
from analytics.settings import db
from datetime import datetime
from .responseHelper import ResponseHelper

res = ResponseHelper()
date_format = '%m-%d-%Y'

class Helper:

    @staticmethod
    def products(x):
        row = {
            'Last Update': x['last_update'].apply(lambda x: datetime.strptime(str(x)[:19], '%Y-%m-%d %H:%M:%S').strftime(date_format)).max(),
            'Total Products': x['units'].count(),
            'totalPrimaryProducts': x['units'].apply(lambda x: x["isPrimary"]==True).count(),
            'totalVarientProducts': x['units'].apply(lambda x: x["isPrimary"]==False).count(),
            'Store Inventory': x['units'].apply(lambda x: int(x["availableQuantity"])).sum(),
        }

        return pd.Series(row)

    @staticmethod
    def count_aggre(x):
        data = {
            "Total Count": x["status"].count(),
            "Active Count": x["status"][x["status"]==1].count(),
            "Inactive Count": x["status"][x["status"]==0].count(),
            'Category Inventory': x['units'].apply(lambda x: int(x["availableQuantity"])).sum(),
        }
        return pd.Series(data)

    @staticmethod
    def brand_aggre(x):
        data = {
            "Total Count": x["status"].count(),
            "Active Count": x["status"][x["status"]==1].count(),
            "Inactive Count": x["status"][x["status"]==0].count(),
            'Brand Inventory': x['units'].apply(lambda x: int(x["availableQuantity"])).sum(),
        }
        return pd.Series(data)

    def get_all_categories(self, query, skip=None, limit=None):
        """
        :param query: query as per category requirements
        :param skip: skip number
        :param limit: limit number
        :return: all categories details as per query
        """
        if skip is not None and limit is not None:
            result = db.category.find(query).skip(int(skip)).limit(limit).sort([("_id", -1)])
        else:
            result = db.category.find(query).sort([("_id", -1)])
        return result

    def get_category_count(self, query):
        """
        :param query: query as per category requirements
        :return: all categories count as per query
        """
        result = db.category.count_documents(query)
        return result

    def stores(self):
        all_stores_df = pd.DataFrame(db.stores.find({"storeId": {"$nin": ["0",0]}, "status":1},{"storeName.en":1}))
        all_stores_df["name"] = all_stores_df["storeName"].apply(lambda x: x.get("en"))
        all_stores_df["id"] = all_stores_df["_id"].astype('str')
        all_stores_df.drop(columns=["storeName","_id"], inplace=True)
        all_stores_df.sort_values("name", ascending=True, inplace=True)

        return all_stores_df.to_dict('records')
    
    def get_leaf_category(self):

        category_list_data = self.find_leaf_category(store_id="", store_category_id="", category_text="", category_id="")

        ##### get all leaf categories #####
        leaf_category_data, leaf_category_ids = self.get_leaf_category_data(category_list_data=category_list_data, store_id="",
                                                    store_category_id="", category_text="")

        final_df = pd.DataFrame(leaf_category_data)
        final_df["name"] = final_df["categoryName"] + " (" + final_df["parentList"] + ")"
        final_df = final_df[["id", "name"]]
        final_df.sort_values("name", ascending=True, inplace=True)

        return final_df.to_dict(orient='records')

    def process_leaf_category_api(self, store_id, store_category_id, category_text, skip, limit):
        """
        this method retuns leaf level categories
        :param limit: for pagination
        :param skip: for pagination
        :param store_id: store id
        :param store_category_id: store category id
        :param category_text: search text
        :return: leaf category data
        """
        ##### -------------------------- get root category data --------------- #####
        # logger.info("inside process_leaf_category_api")
        category_list_data = self.find_leaf_category(store_id, store_category_id, category_text, parent_name_list=None)
        ##### get all leaf categories #####
        leaf_category_data, leaf_category_ids = self.get_leaf_category_data(category_list_data, store_id, store_category_id, category_text)
        leaf_category_data = list(
            filter(lambda i: re.search(category_text.lower(), i["parentList"].lower()), leaf_category_data))
        leaf_category_data = sorted(leaf_category_data, key=lambda x: x.get("name", {}).get("en"))
        leaf_category_data = self.get_leaf_product_count(leaf_category_data, leaf_category_ids, store_id)

        if not leaf_category_data:
            return
        if (skip + limit) <= len(leaf_category_data):
            cat_list = leaf_category_data[skip:skip + limit]
        else:
            cat_list = leaf_category_data[skip:]

        cat_list = leaf_category_data
        final_data = {
            "categoryList": cat_list,
            "totalCount": len(leaf_category_data)
        }

        return final_data

    def find_leaf_category(self, store_id, store_category_id, category_text, category_id="", parent_name_list=None):
        """
        this method gets leaf category from database
        :param store_id: store id
        :param store_category_id: store category id
        :param category_id: category id
        :return: category list
        """
        ##### get category from database #####
        query = {"status": 1}
        if category_id != "":
            query['parentId'] = ObjectId(category_id)
        else:
            if store_category_id != "" and store_category_id != "0":
                query["storeCategory.storeCategoryId"] = store_category_id
            query['parentId'] = {"$exists": False}

        query['storeId'] = "0"

        ##### find child category count from database #####
        category_json = []
        category_data = self.get_all_categories(query)
        for i in category_data:
            child_query = {
                "parentId": ObjectId(str(i['_id'])),
                "status": 1,
                "storeId": "0"
            }

            child_cat_count = self.get_category_count(child_query)

            if parent_name_list is None:
                plist = i['categoryName']['en']
            elif parent_name_list != i['categoryName']['en']:
                plist = f"{parent_name_list}->{i['categoryName']['en']}"
            else:
                plist = parent_name_list
            category_json.append(
                {
                    "id": str(i['_id']),
                    "name": i['categoryName'],
                    "categoryName": i['categoryName']['en'],
                    "childCount": child_cat_count,
                    "Image": i["websiteIcon"],
                    'parentList': plist,
                })

        # logger.info("got the leaf category")
        return category_json

    def get_leaf_category_data(self, category_list_data, store_id, store_category_id, category_text):
        """
        this method recursively calls itself and gets leaf category data
        :param store_category_id: store category id
        :param category_list_data: root category list
        :param store_id: store id
        :return: leaf category list
        """
        leaf_data = []
        leaf_category_ids = []
        for category in category_list_data:
            if category["childCount"] == 0:
                leaf_data.append(category)
                leaf_category_ids.append(category["id"])
            else:
                category_list = self.find_leaf_category(store_id, store_category_id, category_text, category["id"],
                                                        category['parentList'])
                return_data, return_category_ids = self.get_leaf_category_data(category_list, store_id, store_category_id, category_text)
                leaf_data.extend(return_data)
                leaf_category_ids.extend(return_category_ids)
        return leaf_data, leaf_category_ids

    def get_leaf_product_count(self, leaf_category_data, leaf_category_ids, store_id):
        
        query = {"linkedProductCategory.categoryId": {"$in": leaf_category_ids}, "units.availableQuantity": {"$exists": True}}
        projection = {"_id": 0, "linkedProductCategory.categoryId": 1, "status" : 1, "units.availableQuantity": 1}
        if store_id:
            query["storeId"] = ObjectId(store_id)
        data = pd.DataFrame(db.childProducts.find(query,projection))
        if not data.shape[0]:
            data = pd.DataFrame({"status": [10.0], "units": [[{'availableQuantity': 0}]], "linkedProductCategory": [[{'categoryId': ''}]]})
            
        data["units"] = data["units"].apply(lambda x: x[0])
        data["id"] = data["linkedProductCategory"].apply(lambda x: x[0]["categoryId"])
        data.drop(columns="linkedProductCategory", inplace=True)
        data = data.groupby("id").apply(Helper.count_aggre).reset_index()
        leaf_category_data = pd.DataFrame(leaf_category_data)
        final_df = pd.merge(left=leaf_category_data, right=data, left_on="id", right_on="id", how='left')
        final_df['S. No.'] = final_df.reset_index().index + 1
        final_df["Category Name"] = final_df["categoryName"] + " (" + final_df["parentList"] + ")"
        final_df.drop(columns=["id", "name", "childCount", "categoryName", "parentList"], inplace=True)
        final_df = final_df[["S. No.", "image", "Category Name", "Total Count", "Active Count", "Inactive Count", "Category Inventory"]]
        final_df.sort_values("Category Name", ascending=True, inplace=True)
        final_df.fillna(0, inplace=True)

        return final_df.to_dict('records')

    def get_brands_data(self, store_id, skip, limit, category_id):

        # Get Brands name and id
        brands_df = pd.DataFrame(db.brands.find({"status" : 1},{"name": 1}))
        brands_df["name"] = brands_df["name"].apply(lambda x: x["en"])
        brands_df["_id"] = brands_df["_id"].astype(str)

        # Get Child Products for Brands
        query = {"brand": {"$in": list(brands_df["_id"])}}
        projection = {"brandName": 1, "brand": 1, "status" : 1, "units.availableQuantity": 1}
        
        if store_id:
            query["storeId"] = ObjectId(store_id)

        if category_id:
            query["linkedProductCategory.categoryId"] = category_id

        data = pd.DataFrame(db.childProducts.find(query,projection))
        if not data.shape[0]:
            return
        data["units"] = data["units"].apply(lambda x: x[0])
        data = data.groupby("brand", as_index=False).apply(Helper.brand_aggre)

        # Merging Brand Data with Brand Names
        final_df = pd.merge(left=brands_df, right=data,left_on="_id", right_on="brand", how="left")
        final_df.fillna(0, inplace=True)
        final_df.rename(columns={"name": "Brand Name"}, inplace=True)
        final_df.sort_values("Total Count", ascending=False, inplace=True, ignore_index=True)
        final_df['S. No.'] = final_df.reset_index().index + 1
        final_df = final_df[["S. No.", "Brand Name", "Total Count", "Active Count", "Inactive Count", "Brand Inventory"]]
        total_count = final_df.shape[0]

        if (skip + limit) <= total_count:
            final_df = final_df.iloc[skip:skip + limit]
        else:
            final_df = final_df.iloc[skip:]

        final_data = {
            "data": final_df.to_dict(orient="records"),
            "totalCount": total_count
        }

        return final_data
