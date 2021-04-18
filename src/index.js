const express = require('express');
const cors = require('cors');

const port = process.env.PORT || 5001;

// try {
//     setStrWithExInRedis('aa', '3').then((res) => {
//         console.log(res);
//     });
// } catch (err) {
//     console.log(err.message);
// }

const app = express();
app.use(cors());
app.use(express.json());

// app.post('start-scraping', (req, res) => {
//     try {
//         if (!req.body.queueUrl) {
//             return res.status(400).send({
//                 status: 400,
//                 message: 'missing sqs queue url'
//             });
//         }

//         res.status(200).send();
//     } catch (err) {
//         console.log(err);
//         res.status(400).send(err.message);
//     }
// });

// app.post('stop-scraping', (req, res) => {
//     try {
//         if (!req.body.queueUrl) {
//             return res.status(400).send({
//                 status: 400,
//                 message: 'missing sqs queue url'
//             });
//         }
//     } catch (err) {
//         console.log(err);
//         res.status(400).send(err.message);
//     }

// });

app.listen(port, () => {
    console.log(`Server connected to port: ${port}`);
});