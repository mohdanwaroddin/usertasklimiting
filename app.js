const cluster = require('cluster');
const os = require('os');

if (cluster.isMaster) {
    const numWorkers = 2;

    console.log(`Master ${process.pid} is running`);

    // Fork workers.
    for (let i = 0; i < numWorkers; i++) {
        cluster.fork();
    }

    cluster.on('exit', (worker, code, signal) => {
        console.log(`Worker ${worker.process.pid} died`);
        cluster.fork(); // Ensure that a new worker is created if one dies
    });
} else {
    const express = require('express');
    const fs = require('fs');
    const RateLimiterFlexible = require('rate-limiter-flexible');

    const app = express();
    app.use(express.json());

    const PORT = process.env.PORT || 3000;

    // Rate limiters
    const rateLimiter = new RateLimiterFlexible.RateLimiterMemory({
        points: 20, // 20 tasks per minute
        duration: 60, // per minute
        keyPrefix: 'user_rate_limit'
    });

    const rateLimiterPerSec = new RateLimiterFlexible.RateLimiterMemory({
        points: 1, // 1 task per second
        duration: 1, // per second
        keyPrefix: 'user_rate_limit_per_sec'
    });

    // In-memory queue to hold tasks
    const taskQueue = {};

    // Function to process tasks
    const processTask = async (user_id) => {
        console.log(`${user_id} - task completed at - ${Date.now()}`);
        fs.appendFileSync('task_logs.txt', `${user_id} - task completed at - ${new Date().toISOString()}\n`);
    };

    // Function to handle the task queue for a user
    const handleTaskQueue = (user_id) => {
        if (!taskQueue[user_id] || taskQueue[user_id].length === 0) return;

        const nextTask = taskQueue[user_id][0];

        rateLimiterPerSec.consume(user_id)
            .then(() => {
                processTask(user_id);
                taskQueue[user_id].shift(); // Remove the processed task

                if (taskQueue[user_id].length > 0) {
                    setTimeout(() => handleTaskQueue(user_id), 1000); // Process the next task after 1 second
                }
            })
            .catch(() => {
                setTimeout(() => handleTaskQueue(user_id), 1000); // Retry after 1 second if rate limit is hit
            });
    };

    app.post('/api/v1/task', async (req, res) => {
        const { user_id } = req.body;

        if (!user_id) {
            return res.status(400).json({ error: 'User ID is required' });
        }

        try {
            await rateLimiter.consume(user_id); // Consume a point for rate limit check

            if (!taskQueue[user_id]) {
                taskQueue[user_id] = [];
            }

            taskQueue[user_id].push(req.body); // Add task to the user's queue

            if (taskQueue[user_id].length === 1) {
                handleTaskQueue(user_id); // Start processing if this is the first task
            }

            res.status(202).json({ message: 'Task accepted and queued for processing' });
        } catch (rejRes) {
            res.status(429).json({ error: 'Rate limit exceeded. Task queued for processing' });
        }
    });

    app.listen(PORT, () => {
        console.log(`Worker ${process.pid} started on port ${PORT}`);
    });
}
