const express = require('express')
const bodyParser = require('body-parser')
const MongoClient = require('mongodb').MongoClient
const app = express()

// Replace process.env.DB_URL with your actual connection string
const connectionString = process.env.DB_URL || 'mongodb://localhost:27017'

MongoClient.connect(connectionString, { useUnifiedTopology: true })
    .then(client => {
        console.log('Connected to Database')
        const db = client.db('indegene-assignment')
        const authorsCollection = db.collection('authors')
        const awardsCollection = db.collection('awards')
        const booksCollection = db.collection('books')

        // Middlewares
        app.use(bodyParser.urlencoded({ extended: true }))
        app.use(bodyParser.json())
        app.use(express.static('public'))

        // Routes
        // task 1
        app.get('/taskOne', async (req, res) => {
            try {
                let numbe_of_award = Number(req.query.award);
                let awards = await awardsCollection.distinct('_id', { totalAwards: { $gte: numbe_of_award } });
                if (awards.length == 0) return res.status(400).json('No data found.');
                let authors = await authorsCollection.find({ _id: { $in: awards } });
                if (authors.length == 0) {
                    res.status(400).json('No author found.')
                } else {
                    res.status(200).json(authors);
                }
            } catch (e) {
                res.status(400).json('Something went wrong...')
                console.log(e);
            }
        })

        // task 2
        app.get('/taskTwo', async (req, res) => {
            try {
                let year = new Date(req.query.year);
                console.log(year)
                let awards = await awardsCollection.distinct('_id', { date: { $gte: year } });
                if (awards.length == 0) return res.status(400).json('No data found.');
                let authors = await authorsCollection.find({ _id: { $in: awards } });
                if (authors.length == 0) {
                    res.status(400).json('No author found.')
                } else {
                    res.status(200).json(authors);
                }
            } catch (e) {
                res.status(400).json('Something went wrong...')
                console.log(e);
            }
        })

        // task 3
        app.get('/taskThree', async (req, res) => {
            try {
                let authors = await authorsCollection.aggregate([
                    { $group: { _id: '$authorName', totalBooksSold: { $sum: '$quantity' }, totalProfit: { $multiply: ['$profit', '$quantity'] } } },
                    {
                        $project: {
                            'totalBooksSold': 1,
                            'totalProfit': 1
                        }
                    }
                ]);
                if (authors.length == 0) {
                    res.status(400).json('No author found.')
                } else {
                    res.status(200).json(authors);
                }
            } catch (e) {
                res.status(400).json('Something went wrong...')
                console.log(e);
            }
        })

        // task 4
        app.get('/taskFour', async (req, res) => {
            try {
                let date = new Date(req.query.date);
                let totalPrice = req.query.totalPrice;
                let authors = await booksCollection.aggregate([
                    { $group: { _id: '$authorName', totalPrice: { $multiply: ['$price', '$quantity'] } } },
                    { $match: { $and: [{ birthDate: { $gte: date } }, { totalPrice: { $gte: totalPrice } }] } },
                    {
                        $project: {
                            'totalPrice': 1
                        }
                    }
                ]);
                if (authors.length == 0) {
                    res.status(400).json('No author found.')
                } else {
                    res.status(200).json(authors);
                }
            } catch (e) {
                res.status(400).json('Something went wrong...')
                console.log(e);
            }
        })

        
        // Listen
        const port = process.env.NODE_ENV || 3000
        app.listen(port, function () {
            console.log(`listening on ${port}`)
        })
    })
    .catch(console.error)
