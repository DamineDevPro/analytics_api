
const Joi = require('@hapi/joi')
const ObjectID = require('mongodb').ObjectID
const Async = require('async')

const error = require('../../../../locales')
const masterOrderDB = require('../../../../models/masterOrder')
const storeDb = require('../../../../models/stores')
const storeOrderDB = require('../../../../models/storeOrder')
const negotiation = require('../../../../models/negotiation')
const deliveryOrderDB = require('../../../../models/deliveryOrder')
const { errorLogger, debugLogger } = require('../../../../utils/logger')

const handler = async (req, h) => {
  try {
    debugLogger.debug('query data : ', req.query)
    debugLogger.debug('credentials data : ', req.auth.credentials)
    let cond = {}
    const searchCond = {}
    let condArr = [
      {
        $match: cond
      }
    ]
    let collection = masterOrderDB
    let collectionName = 'masterOrder'

    let cityArr = []
    if (cityArr.length === 0 && req.auth.credentials.metaData && req.auth.credentials.metaData.cities) {
      req.auth.credentials.metaData.cities.map((item) => {
        if (ObjectID.isValid(item.cityId)) {
          cityArr.push(item.cityId.toString())
        }
      })
    }

    switch (req.auth.credentials.sub) {
      case 'user':
      case 'userJWT':
        cond = {
          customerId: req.auth.credentials._id,
          'status.paymentFailed': { $ne: true }
        }
        req.query.batchWiseOrders = false
        // req.query.groupByMaster = false
        // req.query.groupByParent = false
        break
      case 'admin':
        cond = {}
        collection = storeOrderDB
        collectionName = 'storeOrder'
        if (req.query.userId !== '') {
          cond = {
            customerId: req.query.userId
          }
        }
        break
      case 'seller':
      case 'manager':
        if (req.auth.credentials.metaData.storeId === '0') {
          cond = {}
          if (req.query.userId !== '') {
            cond = {
              customerId: req.query.userId
            }
          }
        } else if (parseInt(req.auth.credentials.metaData.storeFrontTypeId) === 5) {
          cond = {
            bookingType: 3,
            'DCDetails.id': req.auth.credentials.metaData.storeId
          }
        } else {
          cityArr = []
          cond = {
            storeId: req.auth.credentials.metaData.storeId
          }
          if (req.query.userId !== '') {
            cond.customerId = req.query.userId
          }
          if (req.auth.credentials.metaData.roleId === 4) {
            cond['pickerDetails.id'] = req.auth.credentials._id
          } else if (req.auth.credentials.metaData.roleId === 5) {
            cond['checkerDetails.id'] = req.auth.credentials._id
          } else if (req.auth.credentials.metaData.roleId === 6) {
            cond['comptrollerDetails.id'] = req.auth.credentials._id
          }
        }
        collection = storeOrderDB
        collectionName = 'storeOrder'
        break
      default:
        req.query.batchWiseOrders = false
        req.query.groupByMaster = false
        req.query.groupByParent = false
        cond = {}
    }

    if (parseInt(req.query.storeIdCheck) === 1) {
      const storeD = await storeDb.getOne({ buyerAccountId: String(req.auth.credentials._id) })
      if (storeD && storeD._id) {
        req.auth.credentials.sub = 'manager'
        cityArr = []
        cond = {
          storeId: String(storeD._id)
        }
      }
      collection = storeOrderDB
      collectionName = 'storeOrder'
    }

    if (parseInt(req.query.requestFrom) === 1 && (req.auth.credentials.sub === 'user' || req.auth.credentials.sub === 'userJWT')) {
      req.auth.credentials.sub = 'manager'
      collection = storeOrderDB
      collectionName = 'storeOrder'
    }

    if (parseInt(req.query.storeIdCheck) === 1) {
      const storeD = await storeDb.getOne({ buyerAccountId: String(req.auth.credentials._id) })
      if (storeD && storeD._id) {
        req.auth.credentials.sub = 'manager'
        cityArr = []
        cond = {
          storeId: String(storeD._id)
        }
      }
      collection = storeOrderDB
      collectionName = 'storeOrder'
    }
    if (parseInt(req.query.recepientIdCheck) === 1) {
      cond = {
        'recepientDetails.recepientId': String(req.auth.credentials._id)
      }
    }

    if (req.query.cityId !== '') {
      cityArr = []
      req.query.cityId.split(',').map((item) => {
        if (ObjectID.isValid(item)) {
          cityArr.push(item.toString())
        }
      })
    }

    // if (req.query.referralUserId !== "" && typeof req.query.referralUserId !== "undefined") {
    //   cond.referralUserId = req.query.referralUserId
    // }

    if (cityArr.length > 0) {
      if ((req.query.storeType !== 23)) {
        cond.$or = [
          {
            'sellers.pickupAddress.cityId': {
              $in: cityArr
            }
          },
          {
            'pickupAddress.cityId': {
              $in: cityArr
            }
          }
        ]
      }
    }

    if (req.query.status !== 0) {
      cond['status.status'] = req.query.status
      if (req.query.status === -1) {
        cond['status.status'] = 0
      }
      if (req.query.status === 37) {
        cond['status.status'] = { $in: [3, 7] }
      }
      if (req.query.status === 521) {
        cond['status.status'] = { $in: [5, 21] }
      }
      if (req.query.status === 11 && req.auth.credentials.metaData && parseInt(req.auth.credentials.metaData.storeFrontTypeId) === 5) {
        cond['status.status'] = { $in: [11, 8] }
      }
      if ([1118].indexOf(req.query.status) !== -1) {
        cond['status.status'] = { $in: [11, 18] }
      }
      if ([1920].indexOf(req.query.status) !== -1) {
        cond['status.status'] = { $in: [19, 20] }
      }
      if (req.query.status === 12456) {
        cond['status.status'] = { $in: [1, 2, 4, 5, 6, 8, 9, 10, 11] }
        if (['user', 'userJWT'].indexOf(req.auth.credentials.sub) === -1) {
          cond['status.status'] = { $in: [1, 2] }
        }
      }
    }

    if (req.query.orderType !== 0) {
      if ((req.query.status === 7 || req.query.status === 13132324 || req.query.status === 37 || req.query.status === 4 || req.query.status === 5 || req.query.status === 6 || req.query.status === 13 || req.query.status === 23 || req.query.status === 24) && req.query.orderType === 4) {
        req.query.orderType = 3
      }
      cond.orderType = req.query.orderType
    } else if (req.query.status === 7 || req.query.status === 37) {
      cond.orderType = { $ne: 3 }
      if (['user', 'userJWT'].indexOf(req.auth.credentials.sub) === -1) {
        cond.orderType = { $nin: [3, 5, 6] }
      }
    } else if (parseInt(req.query.isVirtualOrders) === 1) {
      cond.orderType = { $in: [5, 6] }
    } else {
      if ((['user', 'userJWT'].indexOf(req.auth.credentials.sub) === -1) && parseInt(req.query.storeIdCheck) !== 1) {
        cond.orderType = { $nin: [5, 6] }
      }
    }

    if (req.query.storeType !== 0) {
      cond.storeType = req.query.storeType
    } else {
      cond.storeType = { $nin: [23] }
    }

    if (typeof req.query.storeId !== 'undefined' && req.query.storeId !== '') {
      cond.storeId = req.query.storeId
    }
    if (typeof req.query.paymentType !== 'undefined' && req.query.paymentType !== 0) {
      cond.paymentType = req.query.paymentType
    }
    if (typeof req.query.driverId !== 'undefined' && req.query.driverId !== '') {
      cond['driverDetails.driverId'] = new ObjectID(req.query.driverId)
    }
    if (req.query.storeCategoryId !== '') {
      cond.storeCategoryId = req.query.storeCategoryId
    }

    if (req.query.slotId !== '') {
      req.query.bookingType = 3
      const slotCond = [
        {
          slotId: req.query.slotId
        },
        {
          pickupSlotId: req.query.slotId
        },
        {
          deliverySlotId: req.query.slotId
        }
      ]

      if (typeof cond.$or === 'undefined') {
        cond.$or = slotCond
      } else {
        if (typeof cond.$and !== 'undefined') {
          cond.$and.push(slotCond)
        } else {
          cond.$and = [
            { $or: cond.$or },
            { $or: slotCond }
          ]
        }
      }
    }

    if (req.query.bookingType !== 0) {
      if (req.query.bookingType === 23) {
        cond.bookingType = { $in: [2, 3] }
      } else {
        cond.bookingType = req.query.bookingType
      }
    }
    if (req.query.pickupTime !== 0) {
      if (req.query.pickupTime === 1) {
        cond.bookingType = 1
      } else {
        cond.bookingType = 2
      }
    }
    if (req.query.singleTruck !== 0) {
      if (req.query.singleTruck === 1) {
        cond.singleTruck = true
      } else {
        cond.singleTruck = false
      }
    }
    if (req.query.multiStop !== 0) {
      if (req.query.multiStop === 1) {
        cond.multiStop = true
      } else {
        cond.multiStop = false
      }
    }
    if (req.query.vehicleTypeName !== '') {
      cond.vehicleTypeId = req.query.vehicleTypeName
    }
    if (req.query.orderBy !== 0) {
      if (req.query.storeType !== 23) {
        cond['customerDetails.userType'] = req.query.orderBy
      } else {
        cond['customerDetails.institutionType'] = req.query.orderBy
      }
    }

    if (req.query.orderTime !== '') {
      const timeArr = req.query.orderTime.split('-')
      if (parseFloat(timeArr[0]) >= 0) {
        cond.createdTimeStamp = {
          $gte: parseFloat(timeArr[0])
        }
      }
      if (timeArr[1] && parseFloat(timeArr[1]) >= parseFloat(timeArr[0])) {
        cond.createdTimeStamp = {
          $gte: parseFloat(timeArr[0]),
          $lte: parseFloat(timeArr[1])
        }
      }
    }
    if (req.query.cityFromTo !== '') {
      const timeArr = req.query.cityFromTo.split('-')
      if (timeArr[0] >= 0) {
        cond['pickupAddress.cityId'] = timeArr[0]
      }
      if (timeArr[1] && timeArr[1] >= timeArr[0]) {
        cond['pickupAddress.cityId'] = timeArr[0]
        cond['deliveryAddress.cityId'] = timeArr[1]
      }
    }

    if (req.query.search !== '') {
      const regexValue = '.*' + req.query.search + '.*'
      const regEx = { $regex: regexValue, $options: 'i' }
      const condSearch = [
        { orderId: regEx },
        { masterOrderId: regEx },
        { storeOrderId: regEx },
        { packageId: regEx },
        { storeName: regEx },
        { storeAliasName: regEx },
        { 'sellers.name': regEx },
        { 'orders.storeOrderId': regEx },
        { 'products.productOrderId': regEx },
        { 'products.packageId': regEx },
        { 'orders.productOrderId': regEx },
        { 'customerDetails.firstName': regEx },
        { 'customerDetails.lastName': regEx },
        { 'customerDetails.mobile': regEx },
        { 'customerDetails.email': regEx },
        { 'storeOrders.products.name': regEx },
        { 'orders.customerDetails.firstName': regEx },
        { 'orders.customerDetails.lastName': regEx },
        { 'orders.customerDetails.mobile': regEx },
        { 'orders.customerDetails.email': regEx }
      ]
      searchCond.$or = condSearch
      // if (typeof cond.$or === 'undefined') {
      //   cond.$or = condSearch
      // } else {
      //   if (typeof cond.$and !== 'undefined') {
      //     cond.$and.push({ $or: condSearch })
      //   } else {
      //     cond.$and = [
      //       { $or: cond.$or },
      //       { $or: condSearch }
      //     ]
      //   }
      // }
    }
    condArr[0].$match = cond // JSON.parse(JSON.stringify(cond))

    let countByProduct = false
    let userIndex = ['user', 'userJWT'].indexOf(req.auth.credentials.sub)
    if (req.query.storeType === 23 && req.query.status === 5) {
      userIndex = ['userJWT'].indexOf(req.auth.credentials.sub)
    }
    if (userIndex === -1) {
      if ([3, 22].indexOf(req.query.status) !== -1 && req.query.storeType !== 23) {
        collection = storeOrderDB
        collectionName = 'productOrder'
        delete cond['status.status']
        if (req.query.referralUserId !== '' && typeof req.query.referralUserId !== 'undefined') {
          cond['products.referralUserId'] = req.query.referralUserId
        }
        cond['products.status.status'] = req.query.status
        if (req.query.status === 37) {
          cond['products.status.status'] = { $in: [3, 7] }
        }
        if (req.query.status === 22) {
          cond['products.status.status'] = { $in: [22, 10] }
        }
        if (req.query.status === 22) {
          condArr = [
            {
              $match: cond
            }
          ]
        } else {
          countByProduct = true
          condArr = [
            {
              $unwind: '$products'
            },
            {
              $match: cond
            }
          ]
        }
      } else if ([7].indexOf(req.query.status) !== -1 && req.query.calledFor === 1) {
        // collection = storeOrderDB
        // collectionName = 'storeOrder'
      } else if ([4, 5, 6, 7, 14, 15, 13, 23, 24, 13132324].indexOf(req.query.status) !== -1 && parseInt(req.query.isVirtualOrders) !== 1) {
        const deliveryStatusArr = {
          4: 1,
          5: 2,
          6: 3,
          7: 4,
          14: 2,
          15: 3,
          13: 13,
          23: 23,
          24: 24,
          13132324: 13132324
        }
        if ([14, 15].indexOf(req.query.status) !== -1) {
          cond.orderType = 1
        } else if ([5, 6].indexOf(req.query.status) !== -1) {
          if (req.query.orderType === 0) {
            cond.orderType = { $ne: 1 }
          }
        }
        collection = deliveryOrderDB
        collectionName = 'deliveryOrder'
        cond['status.status'] = deliveryStatusArr[req.query.status]
        if (req.query.status === 13132324) {
          cond['status.status'] = { $in: [1, 3, 13, 23, 24] }
        }
        if (req.query.status === 7) {
          if (req.auth.credentials.metaData.roleId === 4) { // picker
            cond['status.status'] = { $in: [19, 20, 21, 1, 2, 3, 4] }
          } else if (req.auth.credentials.metaData.roleId === 5) { // checker
            cond['status.status'] = { $in: [21, 1, 2, 3, 4] }
            // } else if (req.auth.credentials.metaData.roleId === 6) { // comptroller
            // cond['status.status'] = { $in: [19, 20, 21, 1, 2, 3, 4] }
          }
        }
        if (req.query.bookingAssign === 1 && req.query.status === 5) {
          cond['driverDetails.driverId'] = { $ne: '', $exists: true }
          if (req.query.bookingStarted === 2) {
            cond.jobStarted = true
          } else if (req.query.bookingStarted === 1) {
            cond.jobStarted = false
          }
        } else if (req.query.bookingAssign === 2 && req.query.status === 5) {
          cond['driverDetails.driverId'] = ''
        }
        condArr = [
          {
            $match: cond
          }
        ]
      }
    }

    const countCondArr = JSON.parse(JSON.stringify(cond))

    if (req.query.search !== '') {
      if (typeof cond.$or === 'undefined') {
        countCondArr.$or = searchCond.$or
      } else {
        if (typeof cond.$and !== 'undefined') {
          countCondArr.$and.push(searchCond)
        } else {
          countCondArr.$and = [
            { $or: cond.$or },
            searchCond
          ]
        }
      }
    }
    // countCondArr.push({
    //   $project: {
    //     orderId: '$masterOrderId'
    //   }
    // })

    if (req.auth.credentials.sub === 'user' || req.auth.credentials.sub === 'userJWT') {
      let localField = 'orderId'
      if (collectionName === 'deliveryOrder' || collectionName === 'productOrder') {
        localField = 'masterOrderId'
      }
      condArr.push({
        $lookup: {
          from: 'storeOrder',
          localField: localField,
          foreignField: 'masterOrderId',
          as: 'storeOrders'
        }
      })
    }

    let groupByOrder = false
    if (req.query.groupByMaster || req.query.groupByParent) {
      groupByOrder = true
      let groupBy = ''
      let mOrderId = '$masterOrderId'
      if (collectionName === 'masterOrder') {
        mOrderId = '$orderId'
      }
      if (req.query.groupByMaster && req.query.groupByParent) {
        groupBy = {
          $cond: [
            { $eq: ['$parentOrderId', ''] },
            mOrderId,
            '$parentOrderId'
          ]
        }
      } else if (req.query.groupByMaster) {
        groupBy = mOrderId
      } else if (req.query.groupByParent) {
        groupBy = {
          $cond: [
            { $eq: ['$parentOrderId', ''] },
            mOrderId,
            '$parentOrderId'
          ]
        }
      } else {
        groupByOrder = false
      }
      if (groupByOrder) {
        req.query.batchWiseOrders = false
        condArr.push({
          $group: {
            _id: groupBy,
            sortId: { $first: '$_id' },
            masterOrderId: {
              $first: {
                $cond: [
                  { $or: [{ $eq: ['$parentOrderId', ''] }, { $eq: [req.query.groupByParent, false] }] },
                  mOrderId,
                  ''
                ]
              }
            },
            parentOrderId: {
              $first: {
                $cond: [
                  { $eq: [req.query.groupByParent, true] },
                  '$parentOrderId',
                  ''
                ]
              }
            },
            orders: { $push: '$$ROOT' }
          }
        }, {
          $project: {
            _id: '$sortId',
            masterOrderId: 1,
            parentOrderId: 1,
            orders: 1
          }
        })
      }
    }

    if (req.query.batchWiseOrders) {
      groupByOrder = true
      condArr.push({
        $group: {
          _id: '$batchDetails.batchId',
          batbatchDetails: { $first: '$batchDetails' },
          orders: { $push: '$$ROOT' }
        }
      })
    }

    if (!groupByOrder) {
      condArr.push({
        $lookup: {
          from: 'masterOrder',
          localField: 'masterOrderId',
          foreignField: 'orderId',
          as: 'masterOrder'
        }
      })
    }
    if (req.query.search !== '') {
      condArr.push({
        $match: searchCond
      })
    }
    let sort = {
      _id: -1
    }
    if (req.query.finalTotal !== '' && req.query.finalTotal !== 0) {
      if (req.query.finalTotal === -1) {
        sort = {
          'accounting.finalTotal': -1
        }
      } else {
        sort = {
          'accounting.finalTotal': 1
        }
      }
    }
    condArr.push({
      $sort: sort
    }, {
      $skip: parseInt(req.query.skip, 10) || 0
    }, {
      $limit: parseInt(req.query.limit, 10) || 10
    })

    if ((req.auth.credentials.sub === 'user' || req.auth.credentials.sub === 'userJWT') && req.query.storeType === 23 && collectionName !== 'deliveryOrder') {
      let deliveryOrderLocalField = 'storeOrderId'
      if (collectionName === 'masterOrder') {
        if (groupByOrder) {
          deliveryOrderLocalField = 'orders.storeOrders.storeOrderId'
        } else {
          deliveryOrderLocalField = 'storeOrders.storeOrderId'
        }
      }
      condArr.push({
        $lookup: {
          from: 'deliveryOrder',
          localField: deliveryOrderLocalField,
          foreignField: 'storeOrderId',
          as: 'packingDetails'
        }
      })
      if (req.query.bookingAssign === 1 && req.query.status === 0 && req.query.storeType === 23) {
        condArr.push({
          $match: {
            $or: [
              {
                'packingDetails.status': 5,
                'driverDetails.driverId': { $ne: '', $exists: true }
              },
              {
                'packingDetails.status': { $ne: 5 }
              }
            ]
          }
        })
      }
    }
    debugLogger.debug('condition : ' + JSON.stringify(condArr))

    let orderData = await collection.aggregate(condArr)

    let totalCount = orderData.length
    if (countByProduct) {
      const countCondArrAggregate = [
        {
          $unwind: '$products'
        },
        {
          $match: countCondArr
        },
        {
          $group: {
            _id: null,
            count: { $sum: 1 }
          }
        }
      ]
      const totalCountAggregate = await collection.aggregate(countCondArrAggregate)
      if (totalCountAggregate.length > 0) {
        totalCount = totalCountAggregate[0].count
      }
    } else {
      totalCount = await collection.count(countCondArr)
    }
    // totalCount = countData.length

    if (req.auth.credentials.sub === 'user' || req.auth.credentials.sub === 'userJWT') {
      if (groupByOrder) {
        // const parentOrderData = []
        Async.forEach(orderData, async (parentOrder, cb) => {
          parentOrder.orders = parentOrder.orders.map((masterOrder) => {
            masterOrder.storeOrders = (masterOrder.storeOrders || []).map((storeOrder) => {
              if (!storeOrder.storeRattingData) {
                storeOrder.storeRattingData = {
                  rating: 0,
                  reviewTitle: '',
                  isRated: false,
                  reviewDescription: ''
                }
              }
              if (!storeOrder.driverRattingData) {
                storeOrder.driverRattingData = {
                  rating: 0,
                  reviewTitle: '',
                  isRated: false,
                  reviewDescription: ''
                }
              }
              // if ([18].indexOf(storeOrder.status.status) !== -1) {
              //   storeOrder.status.status = 11
              //   storeOrder.status.statusText = 'Picking Started'
              // }
              if ([19, 20].indexOf(storeOrder.status.status) !== -1) {
                storeOrder.status.status = 8
                storeOrder.status.statusText = 'Picked'
              }
              if ([21].indexOf(storeOrder.status.status) !== -1) {
                storeOrder.status.status = 5
                storeOrder.status.statusText = 'Ready for Pickup'
              }
              const storeOrderData = (({
                storeOrderId,
                conversationId,
                childStoreOrderId,
                recepientDetails,
                customerDetails,
                storeType,
                storeTypeMsg,
                storeCategoryId,
                storeCategory,
                hyperlocal,
                storeName,
                storeAliasName,
                storeLogo,
                storeId,
                fullFilledByDC,
                shopPickerAndPackerBy,
                shopPickerAndPackerByText,
                status,
                recepientStatus,
                pickupAddress,
                deliveryAddress,
                bookingType,
                bookingTypeText,
                requestedFor,
                requestedForTimeStamp,
                deliverySlotId,
                deliverySlotDetails,
                products,
                accounting,
                storeRattingData,
                driverRattingData,
                driverDetails,
                poInvoiceLink,
                dispatchEndTime,
                storeShopifyId,
                shopifyEnable,
                loadType,
                loadTypeText
              }) => ({
                storeOrderId,
                conversationId,
                childStoreOrderId,
                recepientDetails,
                customerDetails,
                storeType,
                storeTypeMsg,
                storeCategoryId,
                storeCategory,
                hyperlocal,
                storeName,
                storeAliasName,
                storeLogo,
                storeId,
                fullFilledByDC,
                shopPickerAndPackerBy,
                shopPickerAndPackerByText,
                status,
                recepientStatus,
                pickupAddress,
                deliveryAddress,
                bookingType,
                bookingTypeText,
                requestedFor,
                requestedForTimeStamp,
                deliverySlotId,
                deliverySlotDetails,
                products,
                accounting,
                storeRattingData,
                driverRattingData,
                driverDetails,
                poInvoiceLink,
                dispatchEndTime,
                storeShopifyId,
                shopifyEnable,
                loadType,
                loadTypeText
              }))(storeOrder)
              return storeOrderData
            })
            const masterOrderData = (({
              orderId,
              createdTimeStamp,
              storeType,
              storeTypeMsg,
              orderType,
              orderTypeMsg,
              storeCategoryId,
              recepientDetails,
              customerDetails,
              storeCategory,
              storeOrders,
              accounting,
              status,
              recepientStatus,
              timestamps,
              singleTruck,
              multiStop,
              vehicleTypeId,
              vehicleTypeName,
              vehicleTypeImage,
              loadType,
              loadTypeText
            }) => ({
              orderId,
              createdTimeStamp,
              storeType,
              storeTypeMsg,
              orderType,
              orderTypeMsg,
              storeCategoryId,
              recepientDetails,
              customerDetails,
              storeCategory,
              storeOrders,
              accounting,
              status,
              recepientStatus,
              timestamps,
              singleTruck,
              multiStop,
              vehicleTypeId,
              vehicleTypeName,
              vehicleTypeImage,
              loadType,
              loadTypeText
            }))(masterOrder)
            return masterOrderData
          })
          if (req.query.storeType === 23 && collectionName !== 'deliveryOrder') {
            var orderSorting = []
            parentOrder.packingDetails = parentOrder.packingDetails.map(dOrder => {
              const dOrderData = {
                masterOrderId: dOrder.masterOrderId,
                storeOrderId: dOrder.storeOrderId,
                packageId: dOrder.packageId,
                driverDetails: dOrder.driverDetails,
                vehicleDetails: dOrder.vehicleDetails,
                activityTimeline: [
                  dOrder.activityTimeline[dOrder.activityTimeline.length - 1]
                ]
              }
              if (dOrder.deliverySeq) {
                orderSorting[dOrder.deliverySeq || 0] = dOrder.masterOrderId
              } else {
                orderSorting.push(dOrder.masterOrderId)
              }
              return dOrderData
            })
            orderSorting = orderSorting.filter(n => n)
            const orderDataSorted = []
            orderSorting.map((mId) => {
              parentOrder.orders.map((masterOrder) => {
                if (masterOrder.orderId === mId) {
                  orderDataSorted.push(masterOrder)
                }
              })
            })
            parentOrder.orders = orderDataSorted
          }
          let cond = {}
          if (parentOrder.parentOrderId) {
            cond = { parentOrderId: parentOrder.parentOrderId }
          } else {
            cond = { masterOrderId: parentOrder.masterOrderId }
          }
          const negotiationDataObj = await negotiation.findOne(cond)
          parentOrder.negotiationData = (negotiationDataObj !== null && typeof negotiationDataObj !== 'undefined') ? negotiationDataObj.negotiation : []
          // parentOrderData.push(parentOrder)
          cb(null)
        }, () => {
          // orderData = parentOrder
        })
      } else {
        orderData = orderData.map((masterOrder) => {
          masterOrder.storeOrders = masterOrder.storeOrders.map((storeOrder) => {
            storeOrder.products = storeOrder.products
              // .filter((productOrder) => (productOrder.isParentProduct === true))
              .map((productOrder) => {
                if (!productOrder.rattingData) {
                  productOrder.rattingData = {
                    rating: 0,
                    reviewTitle: '',
                    isRated: false,
                    reviewDescription: ''
                  }
                }

                // if ([18].indexOf(productOrder.status.status) !== -1) {
                //   productOrder.status.status = 11
                //   productOrder.status.statusText = 'Picking Started'
                // }
                if ([19, 20].indexOf(productOrder.status.status) !== -1) {
                  productOrder.status.status = 8
                  productOrder.status.statusText = 'Picked'
                }
                if ([21].indexOf(productOrder.status.status) !== -1) {
                  productOrder.status.status = 5
                  productOrder.status.statusText = 'Ready for Pickup'
                }

                const productOrderData = (({
                  productOrderId,
                  productId,
                  currencySymbol,
                  centralProductId,
                  unitId,
                  packageId,
                  images,
                  name,
                  quantity,
                  packaging,
                  quoteId,
                  quoteNo,
                  rfqId,
                  rfqNumber,
                  mileStones,
                  sellerMileStones,
                  orderAdjustmentLogs,
                  quoteOrderType,
                  quoteOrderTypeText,
                  customType,
                  customTypeText,
                  rattingData,
                  status,
                  recepientStatus,
                  attributes,
                  singleUnitPrice,
                  accounting,
                  isSplitProduct,
                  productOffers,
                  offerDetails
                }) => ({
                  productOrderId,
                  productId,
                  currencySymbol,
                  centralProductId,
                  unitId,
                  packageId,
                  quoteId,
                  quoteNo,
                  rfqId,
                  rfqNumber,
                  mileStones,
                  sellerMileStones,
                  orderAdjustmentLogs,
                  quoteOrderType,
                  quoteOrderTypeText,
                  customType,
                  customTypeText,
                  images,
                  name,
                  quantity,
                  packaging,
                  rattingData,
                  status,
                  recepientStatus,
                  attributes,
                  singleUnitPrice,
                  accounting,
                  isSplitProduct,
                  productOffers,
                  offerDetails
                }))(productOrder)
                return productOrderData
              })
            if (!storeOrder.storeRattingData) {
              storeOrder.storeRattingData = {
                rating: 0,
                reviewTitle: '',
                isRated: false,
                reviewDescription: ''
              }
            }
            if (!storeOrder.driverRattingData) {
              storeOrder.driverRattingData = {
                rating: 0,
                reviewTitle: '',
                isRated: false,
                reviewDescription: ''
              }
            }
            // if ([18].indexOf(storeOrder.status.status) !== -1) {
            //   storeOrder.status.status = 11
            //   storeOrder.status.statusText = 'Picking Started'
            // }
            if ([19, 20].indexOf(storeOrder.status.status) !== -1) {
              storeOrder.status.status = 8
              storeOrder.status.statusText = 'Picked'
            }
            if ([21].indexOf(storeOrder.status.status) !== -1) {
              storeOrder.status.status = 5
              storeOrder.status.statusText = 'Ready for Pickup'
            }
            const storeOrderData = (({
              storeOrderId,
              conversationId,
              childStoreOrderId,
              recepientDetails,
              customerDetails,
              storeType,
              storeTypeMsg,
              storeCategoryId,
              storeCategory,
              hyperlocal,
              storeName,
              storeAliasName,
              storeLogo,
              storeId,
              fullFilledByDC,
              shopPickerAndPackerBy,
              shopPickerAndPackerByText,
              status,
              recepientStatus,
              pickupAddress,
              deliveryAddress,
              bookingType,
              bookingTypeText,
              requestedFor,
              requestedForTimeStamp,
              deliverySlotId,
              deliverySlotDetails,
              products,
              accounting,
              storeRattingData,
              driverRattingData,
              driverDetails,
              poInvoiceLink,
              dispatchEndTime,
              storeShopifyId,
              shopifyEnable,
              loadType,
              loadTypeText
            }) => ({
              storeOrderId,
              conversationId,
              childStoreOrderId,
              recepientDetails,
              customerDetails,
              storeType,
              storeTypeMsg,
              storeCategoryId,
              storeCategory,
              hyperlocal,
              storeName,
              storeAliasName,
              storeLogo,
              storeId,
              fullFilledByDC,
              shopPickerAndPackerBy,
              shopPickerAndPackerByText,
              status,
              recepientStatus,
              pickupAddress,
              deliveryAddress,
              bookingType,
              bookingTypeText,
              requestedFor,
              requestedForTimeStamp,
              deliverySlotId,
              deliverySlotDetails,
              products,
              accounting,
              storeRattingData,
              driverRattingData,
              driverDetails,
              poInvoiceLink,
              dispatchEndTime,
              storeShopifyId,
              shopifyEnable,
              loadType,
              loadTypeText
            }))(storeOrder)
            return storeOrderData
          })
          const masterOrderData = (({
            orderId,
            createdTimeStamp,
            storeType,
            storeTypeMsg,
            orderType,
            orderTypeMsg,
            storeCategoryId,
            recepientDetails,
            customerDetails,
            storeCategory,
            storeOrders,
            accounting,
            status,
            recepientStatus,
            timestamps,
            singleTruck,
            multiStop,
            vehicleTypeId,
            vehicleTypeName,
            vehicleTypeImage,
            loadType,
            loadTypeText
          }) => ({
            orderId,
            createdTimeStamp,
            storeType,
            storeTypeMsg,
            orderType,
            orderTypeMsg,
            storeCategoryId,
            recepientDetails,
            customerDetails,
            storeCategory,
            storeOrders,
            accounting,
            status,
            recepientStatus,
            timestamps,
            singleTruck,
            multiStop,
            vehicleTypeId,
            vehicleTypeName,
            vehicleTypeImage,
            loadType,
            loadTypeText
          }))(masterOrder)
          return masterOrderData
        })
      }
    } else if (!groupByOrder) {
      orderData = orderData.map((storeOrder) => {
        storeOrder.relatedOrder = 0
        storeOrder.orderTotal = 0
        if (storeOrder.masterOrder && storeOrder.masterOrder.length > 0) {
          storeOrder.relatedOrder = storeOrder.masterOrder[0].orders.length - 1
          storeOrder.orderTotal = storeOrder.masterOrder[0].accounting.finalTotal
        } else {
          errorLogger.error('storeOrder error : ' + JSON.stringify(storeOrder))
        }
        delete storeOrder.masterOrder
        return storeOrder
      })
    } else if (groupByOrder && collectionName === 'storeOrder') {
      orderData.map((batchOrder) => {
        batchOrder.count = {
          paymentPending: 0,
          new: 0,
          pending: 0,
          unavailable: 0,
          packed: 0,
          readyForPickup: 0,
          inDelivery: 0,
          completed: 0,
          substitutes: 0,
          picked: 0,
          pendingReview: 0,
          review: 0
        }
        batchOrder.orders.map((storeOrder) => {
          storeOrder.count = {
            paymentPending: 0,
            new: 0,
            pending: 0,
            unavailable: 0,
            packed: 0,
            readyForPickup: 0,
            inDelivery: 0,
            completed: 0,
            substitutes: 0,
            picked: 0,
            pendingReview: 0,
            review: 0
          }
          storeOrder.products.map((productOrder) => {
            switch (productOrder.status.status) {
              case 0:
                batchOrder.count.paymentPending++
                storeOrder.count.paymentPending++
                break
              case 1:
                batchOrder.count.new++
                storeOrder.count.new++
                break
              case 2:
                batchOrder.count.pending++
                storeOrder.count.pending++
                break
              case 3:
                batchOrder.count.unavailable++
                storeOrder.count.unavailable++
                break
              case 4:
                batchOrder.count.packed++
                storeOrder.count.packed++
                break
              case 5:
                batchOrder.count.readyForPickup++
                storeOrder.count.readyForPickup++
                break
              case 6:
                batchOrder.count.inDelivery++
                storeOrder.count.inDelivery++
                break
              case 7:
                batchOrder.count.completed++
                storeOrder.count.completed++
                break
              case 8:
                batchOrder.count.picked++
                storeOrder.count.picked++
                break
              case 9:
                batchOrder.count.pendingReview++
                storeOrder.count.pendingReview++
                break
              case 10:
                batchOrder.count.review++
                storeOrder.count.review++
                break
            }
            if (productOrder.subsitute) {
              batchOrder.count.substitutes++
              storeOrder.count.substitutes++
            }
          })
        })
      })
    }
    debugLogger.debug(`(${collectionName}) res : ${orderData.length} with total count : ${totalCount}`)
    return h.response({ message: req.i18n.__('genericErrMsg.200'), count: totalCount, data: orderData })
  } catch (err) {
    errorLogger.error('....', err)
    return h.response({ message: req.i18n.__('genericErrMsg.500') }).code(500)
  }
}
const query = {
  userId: Joi.string()
    .optional()
    .default('')
    .allow('')
    .description('user Id to filter order by user'),
  limit: Joi.number()
    .required()
    .description('limit'),
  skip: Joi.number()
    .required()
    .description('skip'),
  status: Joi.number()
    .required()
    .valid([-1, 0, 1, 2, 3, 4, 5, 6, 7, 12456, 37, 8, 9, 11, 14, 15, 16, 18, 19, 20, 21, 521, 1118, 1920, 22, 12, 13, 23, 24, 13132324])
    .description('0-All, 1-New, 2-Accepted, 3-Cancelled, 4-Packed & Ready, 5-Ready for Pickup, 6-In-Delivery, 7-Completed, 12456-Active, 37-Cancelled & Completed, 8-Picked, 9-Pending for customer confirmation, 11-Picking, 14-Ready For Self Pickup, 15-Customer Arrived, 16-Rejected, 18-Picker Assigned, 19-Checker Assign, 20-Checking, 21-Comptroller Assigned, 521-Checked Out, 1118-Picker Assigned and picking, 22-Missing, 12-Return Initiate,13-Return Completed,23-Return Cancelled,24-In QC Return Order, 13132324-Get all return order for seller.'),
  bookingAssign: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1, 2])
    .description('this filter is only for status 5-Ready for Pickup. 0-All, 1-Assign, 2-Not Assign'),
  bookingStarted: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1, 2])
    .description('this filter is only for status 5-Ready for Pickup. 0-Any, 1-No, 2-Yes'),
  bookingType: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1, 2, 3, 23])
    .description('0-All, 1-ASAP, 2-Schedule By Time, 3-Schedule By Slot, 23-Scheduled'),
  storeIdCheck: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1])
    .description('want to check storeId check : 0-False, 1-True'),
  requestFrom: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1])
    .description('want to check request from dispacther for customer portal check : 0-False, 1-True'),
  recepientIdCheck: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1])
    .description('want to check storeId check : 0-False, 1-True'),
  cityId: Joi.string()
    .allow('')
    .optional()
    .default('')
    .description('city id - comma seprated or blank for all'),

  storeId: Joi.string()
    .allow('')
    .optional()
    .default('')
    .description('store id or blank for all'),

  driverId: Joi.string()
    .allow('')
    .optional()
    .default('')
    .description('driver id or blank for all'),

  batchWiseOrders: Joi.boolean()
    .optional()
    .default(false)
    .description('need orders batch wise or not'),

  groupByMaster: Joi.boolean()
    .optional()
    .default(false)
    .description('need orders in grouped by master order. Note : this will be true incase groupByParent is true'),

  groupByParent: Joi.boolean()
    .optional()
    .default(false)
    .description('need orders in grouped by parent order'),

  isVirtualOrders: Joi.number()
    .optional()
    .default(0)
    .allow([0, 1])
    .description('0-False, 1-True'),

  orderType: Joi.number()
    .optional()
    .default(0)
    .allow([0, 1, 2, 3, 4, 5, 6, 7])
    .description('0-all, 1-Pickup, 2-Delivery, 3-Voucher, 4-Return, 5-VideoShoutout, 6-VideoCall, 6-HookUpOrder'),

  paymentType: Joi.number()
    .optional()
    .default(0)
    .allow([0, 1, 2])
    .description('0-all, 1-Online Payment, 2-Cash'),

  storeType: Joi.number()
    .optional()
    .default(0)
    .allow([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 23])
    .description('0-all, 1-food, 2-grocery, 3-fashion, 4-sendPackages, 5-laundry, 6-pharmacy, 7-e-commerceAffiliate, 8-e-CommercePartner, 9-orderAnything, 10-Meat, 11-Liquor 12-grocery nau, 13- Cannabis, 23-Trucker'),
  storeCategoryId: Joi.string()
    .optional()
    .default('')
    .allow('')
    .description('filter orders with store category.'),
  slotId: Joi.string()
    .optional()
    .default('')
    .allow('')
    .description('filter orders with slot.'),
  referralUserId: Joi.string()
    .optional()
    .default('')
    .allow('')
    .description('filter orders with Referral User.'),
  orderBy: Joi.number()
    .optional()
    .default(0)
    .allow([0, 1, 2])
    .description('0-all, 1-Retailer, 2-distributor'),
  calledFor: Joi.number()
    .optional()
    .default(0)
    .allow([0, 1])
    .description('0-DeliveryOrder, 1-storeOrder (only for accounting page in superadmin and storeadmin'),
  orderTime: Joi.string()
    .optional()
    .default('')
    .allow('')
    .description('from date - to date(timestamp seprated by -) eg.(1578313576-1578313576)'),
  search: Joi.string()
    .optional()
    .default('')
    .allow('')
    .description('search with text on customer name, number, email, central order id, orderId,  returnOrderId'),
  pickupTime: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1, 2])
    .description('0-All, 1-ASAP, 2-Schedule By Time'),
  vehicleTypeName: Joi.string()
    .allow('')
    .optional()
    .default('')
    .description('vehicle id - comma seprated or blank for all'),
  singleTruck: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1, 2])
    .description('0-All, 1-single Truck , 2- multi Truck'),
  multiStop: Joi.number()
    .optional()
    .default(0)
    .valid([0, 1, 2])
    .description('0-All, 1-single, 2- multi'),
  finalTotal: Joi.number()
    .optional()
    .default(0)
    .valid([-1, 0, 1])
    .description(' 1 asc, -1 desc'),
  cityFromTo: Joi.string()
    .optional()
    .default('')
    .allow('')
    .description('from city - to city(timestamp seprated by -) eg.(cityid-cityid)')
}

/**
 * response validation
 */
const response = {
  status: {
    200: {
      message: error.slaveSignIn['200'],
      count: Joi.number(),
      data: Joi.any()
    },
    401: { message: Joi.any() },
    500: { message: error.genericErrMsg['500'] }
  },
  failAction: 'log'
}

module.exports = { handler, query, response }