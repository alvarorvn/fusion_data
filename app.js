const mongodb = require('mongodb')
const async = require('async')
const data = require('./m3-customer-data.json')
const dataAddress = require('./m3-customer-address-data.json')
const numObjects = parseInt(process.argv[2]) || data.length
const url = 'mongodb://localhost:27017'

let task = []

mongodb.MongoClient.connect(url, {useNewUrlParser: true }, (err, client)=>{
    if(err){
        console.log(err)
        process.exit(1)
    }else{
        console.log('Connected to MongoDB server...')
        console.log(`Launching ${task.length} parallel task(s)`)
        const db = client.db('fusion_data')

        
        if(data.length % numObjects == 0){
            data.forEach((custom,index)=>{  
                custom = Object.assign(custom, dataAddress[index])
                if(index % numObjects == 0){
                    var lot = index
                    lot = (lot + numObjects > data.length) ? data.length -1 : lot+numObjects
                    if(lot > data.length){
                        lot = data.length
                    }
                    task.push((callback)=>{
                        console.log(`Processing ${index}-${lot} of ${data.length}`)
                        db.collection('total-data').insertMany(data.slice(index, lot), (error) => {
                            callback(error)
                        });
                    })
                }                                  
            })
        }else{
            console.log("It's not possible to determine a number of round lots")
            process.exit(1)
        }        
    }

    async.parallel(task, (error)=>{
        if(error){
            console.error(error)
        }
        console.log('Completed')
        client.close()
    })
})