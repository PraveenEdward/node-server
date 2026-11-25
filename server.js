
require('dotenv').config({ path: './database.env' });

const express = require('express');
const mysql = require('mysql2/promise');
const path = require('path');
const crypto = require('crypto');

const {
    DynamoDBClient,
    DescribeTableCommand
} = require('@aws-sdk/client-dynamodb');

const {
    DynamoDBDocumentClient,
    ScanCommand,
    GetCommand,
    PutCommand,
    UpdateCommand,
    DeleteCommand
} = require('@aws-sdk/lib-dynamodb');

const app = express();

// ==========================================
// APPLICATION CONFIGURATION
// ==========================================

const PORT = process.env.PORT || 3000;

const DB_TYPE = (process.env.DB_TYPE || 'dynamodb').toLowerCase();

const TABLE_NAME = process.env.DYNAMODB_TABLE || 'names';

const AWS_REGION = process.env.AWS_REGION || 'ap-southeast-1';

let dbReady = false;
let reconnectTimer = null;

// ==========================================
// MIDDLEWARE
// ==========================================

app.use(express.json());

app.use(express.static(path.join(__dirname, 'public')));

// ==========================================
// DATABASE CONFIGURATION
// ==========================================

// RDS CONFIGURATION

let rds = null;

const rdsConfig = {
    host: process.env.DB_HOST,
    user: process.env.DB_USER,
    password: process.env.DB_PASS,
    database: process.env.DB_NAME,
    port: Number(process.env.DB_PORT || 3306),
    waitForConnections: true,
    connectionLimit: 10,
    queueLimit: 0
};

// DYNAMODB CONFIGURATION

const dynamoClient = new DynamoDBClient({
    region: AWS_REGION
});

const dynamo = DynamoDBDocumentClient.from(dynamoClient);

// ==========================================
// DYNAMODB COUNTER INITIALIZATION
// ==========================================

async function initializeCounter() {

    // Check whether counter already exists

    const existing = await dynamo.send(
        new GetCommand({
            TableName: TABLE_NAME,
            Key: {
                id: 'COUNTER'
            },
            ConsistentRead: true
        })
    );

    if (existing.Item) {

        console.log(
            'DynamoDB counter already exists:',
            existing.Item.value
        );

        return;
    }

    // Find the highest existing numeric ID

    let lastKey = undefined;
    let maxId = 0;

    do {

        const result = await dynamo.send(
            new ScanCommand({
                TableName: TABLE_NAME,
                ConsistentRead: true,
                ExclusiveStartKey: lastKey
            })
        );

        for (const item of result.Items || []) {

            if (item.id === 'COUNTER') {
                continue;
            }

            const numericId = Number(item.id);

            if (
                Number.isSafeInteger(numericId) &&
                numericId > maxId &&
                String(numericId) === String(item.id)
            ) {
                maxId = numericId;
            }
        }

        lastKey = result.LastEvaluatedKey;

    } while (lastKey);

    // Create counter only if it does not exist

    try {

        await dynamo.send(
            new PutCommand({
                TableName: TABLE_NAME,

                Item: {
                    id: 'COUNTER',
                    value: maxId
                },

                ConditionExpression:
                    'attribute_not_exists(id)'
            })
        );

        console.log(
            'DynamoDB counter initialized:',
            maxId
        );

    } catch (err) {

        if (
            err.name ===
            'ConditionalCheckFailedException'
        ) {
            console.log('Counter was initialized by another process');
            return;
        }

        throw err;
    }
}

// ==========================================
// CONNECT TO DATABASE
// ==========================================

async function connectDatabase() {

    try {

        dbReady = false;

        console.log(
            `Connecting to ${DB_TYPE.toUpperCase()}...`
        );

        // --------------------------------------
        // CONNECT TO RDS MYSQL
        // --------------------------------------

        if (DB_TYPE === 'rds') {

            if (!rds) {
                rds = mysql.createPool(rdsConfig);
            }

            // Verify MySQL connection

            await rds.query('SELECT 1');

            console.log(
                'Connected to RDS MySQL successfully'
            );

            // Create table automatically

            await rds.query(`
                CREATE TABLE IF NOT EXISTS names (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    name VARCHAR(255) NOT NULL
                )
            `);

            console.log(
                'RDS table "names" is ready'
            );

        }

        // --------------------------------------
        // CONNECT TO DYNAMODB
        // --------------------------------------

        else if (DB_TYPE === 'dynamodb') {

            await dynamo.send(
                new DescribeTableCommand({
                    TableName: TABLE_NAME
                })
            );

            console.log(
                'Connected to DynamoDB successfully'
            );

            // Initialize sequential ID counter

            await initializeCounter();

            console.log(
                `DynamoDB table "${TABLE_NAME}" is ready`
            );

        }

        else {

            throw new Error(
                'Invalid DB_TYPE. Use rds or dynamodb'
            );
        }

        dbReady = true;

        console.log('Database is ready');

    } catch (err) {

        dbReady = false;

        console.error(
            'Database connection failed:',
            err.name,
            err.message
        );

        // Close failed RDS pool before retry

        if (DB_TYPE === 'rds' && rds) {

            await rds.end().catch(() => {});

            rds = null;
        }

        console.log(
            'Retrying database connection in 5 seconds...'
        );

        if (!reconnectTimer) {

            reconnectTimer = setTimeout(() => {

                reconnectTimer = null;

                connectDatabase();

            }, 5000);
        }
    }
}

// Start database connection

connectDatabase();

// ==========================================
// DATABASE MIDDLEWARE
// ==========================================

function checkDatabase(req, res, next) {

    if (!dbReady) {

        return res.status(503).json({
            error: 'Database is not ready'
        });
    }

    next();
}

// ==========================================
// HOMEPAGE
// ==========================================

app.get('/', (req, res) => {

    res.sendFile(
        path.join(__dirname, 'public', 'index.html')
    );
});

// ==========================================
// HEALTH CHECK
// ==========================================

app.get('/health', (req, res) => {

    if (!dbReady) {

        return res.status(503).json({
            status: 'unhealthy',
            database: 'not ready',
            type: DB_TYPE
        });
    }

    res.json({
        status: 'healthy',
        database: 'connected',
        type: DB_TYPE
    });
});

// ==========================================
// GET ALL NAMES
// ==========================================

app.get('/names', checkDatabase, async (req, res) => {

    try {

        let results = [];

        // RDS

        if (DB_TYPE === 'rds') {

            const [rows] = await rds.query(
                'SELECT * FROM names ORDER BY id ASC'
            );

            results = rows;
        }

        // DYNAMODB

        else {

            let lastKey = undefined;

            do {

                const result = await dynamo.send(
                    new ScanCommand({

                        TableName: TABLE_NAME,

                        ExclusiveStartKey: lastKey,

                        FilterExpression:
                            'id <> :counter',

                        ExpressionAttributeValues: {
                            ':counter': 'COUNTER'
                        }
                    })
                );

                results.push(...(result.Items || []));

                lastKey = result.LastEvaluatedKey;

            } while (lastKey);

            results.sort((a, b) => {

                const idA = Number(a.id);
                const idB = Number(b.id);

                if (
                    Number.isSafeInteger(idA) &&
                    Number.isSafeInteger(idB)
                ) {
                    return idA - idB;
                }

                return String(a.id).localeCompare(String(b.id));
            });
        }

        res.json(results);

    } catch (err) {

        console.error('GET error:', err);

        res.status(500).json({
            error: 'Failed to fetch names'
        });
    }
});

// ==========================================
// GET NAME BY ID
// ==========================================

app.get('/names/:id', checkDatabase, async (req, res) => {

    const { id } = req.params;

    try {

        let item = null;

        // RDS

        if (DB_TYPE === 'rds') {

            const [rows] = await rds.query(
                'SELECT * FROM names WHERE id = ?',
                [id]
            );

            item = rows[0] || null;
        }

        // DYNAMODB

        else {

            const result = await dynamo.send(
                new GetCommand({

                    TableName: TABLE_NAME,

                    Key: {
                        id: id
                    }
                })
            );

            item = result.Item || null;
        }

        if (!item || item.id === 'COUNTER') {

            return res.status(404).json({
                error: 'Name not found'
            });
        }

        res.json(item);

    } catch (err) {

        console.error('GET error:', err);

        res.status(500).json({
            error: 'Failed to fetch name'
        });
    }
});

// ==========================================
// CREATE NAME
// ==========================================

app.post('/names', checkDatabase, async (req, res) => {

    const { name } = req.body;

    if (
        typeof name !== 'string' ||
        !name.trim()
    ) {

        return res.status(400).json({
            error: 'Name is required'
        });
    }

    try {

        let id;

        // RDS AUTO_INCREMENT

        if (DB_TYPE === 'rds') {

            const [result] = await rds.query(
                'INSERT INTO names (name) VALUES (?)',
                [name.trim()]
            );

            id = result.insertId;
        }

        // DYNAMODB SEQUENTIAL COUNTER

        else {

            const counter = await dynamo.send(
                new UpdateCommand({

                    TableName: TABLE_NAME,

                    Key: {
                        id: 'COUNTER'
                    },

                    UpdateExpression:
                        'ADD #value :increment',

                    ExpressionAttributeNames: {
                        '#value': 'value'
                    },

                    ExpressionAttributeValues: {
                        ':increment': 1
                    },

                    ReturnValues: 'UPDATED_NEW'
                })
            );

            id = Number(counter.Attributes.value);

            await dynamo.send(
                new PutCommand({

                    TableName: TABLE_NAME,

                    Item: {
                        id: String(id),
                        name: name.trim()
                    },

                    ConditionExpression:
                        'attribute_not_exists(id)'
                })
            );
        }

        res.status(201).json({
            id: id,
            name: name.trim()
        });

    } catch (err) {

        console.error('INSERT error:', err);

        res.status(500).json({
            error: 'Failed to insert name'
        });
    }
});

// ==========================================
// UPDATE NAME
// ==========================================

app.put('/names/:id', checkDatabase, async (req, res) => {

    const { id } = req.params;

    const { name } = req.body;

    if (
        typeof name !== 'string' ||
        !name.trim()
    ) {

        return res.status(400).json({
            error: 'Name is required'
        });
    }

    try {

        // RDS

        if (DB_TYPE === 'rds') {

            const [result] = await rds.query(
                'UPDATE names SET name = ? WHERE id = ?',
                [name.trim(), id]
            );

            if (result.affectedRows === 0) {

                const [rows] = await rds.query(
                    'SELECT id FROM names WHERE id = ?',
                    [id]
                );

                if (rows.length === 0) {

                    return res.status(404).json({
                        error: 'Name not found'
                    });
                }
            }
        }

        // DYNAMODB

        else {

            await dynamo.send(
                new UpdateCommand({

                    TableName: TABLE_NAME,

                    Key: {
                        id: id
                    },

                    UpdateExpression:
                        'SET #name = :name',

                    ExpressionAttributeNames: {
                        '#name': 'name'
                    },

                    ExpressionAttributeValues: {
                        ':name': name.trim()
                    },

                    ConditionExpression:
                        'attribute_exists(id) AND id <> :counter',

                    ExpressionAttributeValues: {
                        ':name': name.trim(),
                        ':counter': 'COUNTER'
                    },

                    ReturnValues: 'ALL_NEW'
                })
            );
        }

        res.json({
            message: 'Name updated successfully'
        });

    } catch (err) {

        if (
            err.name ===
            'ConditionalCheckFailedException'
        ) {

            return res.status(404).json({
                error: 'Name not found'
            });
        }

        console.error('UPDATE error:', err);

        res.status(500).json({
            error: 'Failed to update name'
        });
    }
});

// ==========================================
// DELETE NAME
// ==========================================

app.delete('/names/:id', checkDatabase, async (req, res) => {

    const { id } = req.params;

    try {

        // RDS

        if (DB_TYPE === 'rds') {

            const [result] = await rds.query(
                'DELETE FROM names WHERE id = ?',
                [id]
            );

            if (result.affectedRows === 0) {

                return res.status(404).json({
                    error: 'Name not found'
                });
            }
        }

        // DYNAMODB

        else {

            await dynamo.send(
                new DeleteCommand({

                    TableName: TABLE_NAME,

                    Key: {
                        id: id
                    },

                    ConditionExpression:
                        'attribute_exists(id) AND id <> :counter',

                    ExpressionAttributeValues: {
                        ':counter': 'COUNTER'
                    }
                })
            );
        }

        res.json({
            message: 'Name deleted successfully'
        });

    } catch (err) {

        if (
            err.name ===
            'ConditionalCheckFailedException'
        ) {

            return res.status(404).json({
                error: 'Name not found'
            });
        }

        console.error('DELETE error:', err);

        res.status(500).json({
            error: 'Failed to delete name'
        });
    }
});

// ==========================================
// START NODE.JS SERVER
// ==========================================

const server = app.listen(PORT, () => {

    console.log(
        `Server running on port ${PORT}`
    );

    console.log(
        `Database type: ${DB_TYPE}`
    );
});

// ==========================================
// GRACEFUL SHUTDOWN
// ==========================================

async function shutdown() {

    console.log('Shutting down application...');

    if (reconnectTimer) {
        clearTimeout(reconnectTimer);
    }

    server.close();

    if (rds) {
        await rds.end().catch(() => {});
    }

    await dynamoClient.destroy();

    process.exit(0);
}

process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
