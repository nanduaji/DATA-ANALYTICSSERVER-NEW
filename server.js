const express = require('express');
const bodyParser = require('body-parser');
const multer = require('multer');
const cors = require('cors');
const Minio = require('minio');
const crypto = require('crypto');
const fs = require('fs');
const fetch = require('node-fetch');
const { exec } = require('child_process');

const minioClient = new Minio.Client({
  endPoint: '172.17.0.2',
  port: 9000,
  useSSL: false,
  accessKey: 'minio1-access-key',
  secretKey: 'minio1-secret-key',
});

const app = express();
const PORT = 3001;
const upload = multer({ dest: 'uploads/' });
app.use(cors());
app.use(bodyParser.json());
app.get('/minio-storage-info', (req, res) => {
  exec("mc admin info alias --json", (error, stdout, stderr) => {
    if (error) {
      console.error(`exec error: ${error}`);
      return res.status(500).send('Error fetching MinIO storage info');
    }
    function formatBytes(bytes, decimals = 2) {
      if (bytes === 0) return '0 Bytes';  
      const k = 1024;
      const dm = decimals < 0 ? 0 : decimals;
      const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'];  
      const i = Math.floor(Math.log(bytes) / Math.log(k));
      return parseFloat((bytes / Math.pow(k, i)).toFixed(dm)) + ' ' + sizes[i];
  }
    const info = JSON.parse(stdout);
  const totalSizeInBytes = info.info.usage.size;
  const formattedSize = formatBytes(totalSizeInBytes);
    res.json(formattedSize);
  });
});

const fetchAnalysisResult = async (filenameData) => {
  try {
    const { fileName } = filenameData;
    const fileExtension = filenameData.split('.').pop();
    console.log("fileExtension", fileExtension);

    const analysisResponse = await fetch('http://10.176.26.230:5000/perform-analysis', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ filename: filenameData, file_type: fileExtension }),
    });

    const analysisResult = await analysisResponse.json();
    analysisResult.filename = filenameData;
    return { filename: filenameData, analysisResult };
  } catch (error) {
    console.error('Error fetching analysis result:', error);
    return { filename: filenameData.fileName, error: 'Failed to fetch analysis result' };
  }
};

app.post('/start-analysis-multiple', async (req, res) => {
  try {
    const { filenames } = req.body;

    const analysisResults = await Promise.all(filenames.map(fetchAnalysisResult));

    res.status(200).json({ analysisResults });
  } catch (error) {
    console.error('Error triggering analysis:', error);
    res.status(500).json({ error: 'Failed to start analysis' });
  }
});

// app.post('/start-analysis-multiple', async (req, res) => {
//   try {
//     const { filenames } = req.body;
//     console.log("filenames",filenames)
//     // for (const fileName of filenames) {
//       const response = await fetch('http://10.176.26.230:5000/perform-analysis-multiple', {
//         method: 'POST',
//         headers: {
//           'Content-Type': 'application/json',
//         },
//         body: JSON.stringify({ filenames,file_type: req.body.file_type }),
//       });
//       const analysisResult = await response.json();
//     res.status(200).json({ analysisResult });
//   } catch (error) {
//     console.error('Error triggering analysis:', error);
//     res.status(500).json({ error: 'Failed to start analysis' });
//   }
// });
function filterUniqueEntities(entities) {
  const uniqueEntities = [];
  const seenLabels = new Set();

  for (const entity of entities) {
    if (!seenLabels.has(entity.label)) {
      uniqueEntities.push(entity);
      seenLabels.add(entity.label);
    }
  }

  return uniqueEntities;
}
app.get('/fetchfromentitywatchlist', async (req, res) => {
  const bucketName = 'entitywatchlist';
  try {
    const objectsStream = minioClient.listObjectsV2(bucketName, '', true);
    const entities = [];
    for await (const obj of objectsStream) {
      entities.push(obj.name);
    }
    res.status(200).json(entities);
  } catch (err) {
    console.error('Error fetching data from entitywatchlist bucket:', err);
    res.status(500).send({ error: 'Failed to fetch data' });
  }
});
app.get('/fetchConflictingEntities', async (req, res) => {
  const bucketName = 'entitywatchlistmatchedentities';

  try {
    const objectsStream = minioClient.listObjects(bucketName, '', true);
    const conflictingEntities = [];

    objectsStream.on('data', obj => {
      // Add each object in the bucket to the conflictingEntities array
      conflictingEntities.push(obj);
    });

    objectsStream.on('error', err => {
      console.error('Error fetching conflicting entities:', err);
      res.status(500).send({ error: 'Failed to fetch conflicting entities' });
    });

    objectsStream.on('end', () => {
      // Send the conflictingEntities array as the response
      res.status(200).send(conflictingEntities);
    });
  } catch (err) {
    console.error('Error fetching conflicting entities:', err);
    res.status(500).send({ error: 'Failed to fetch conflicting entities' });
  }
});

app.post('/saveConflictingEntities', async (req, res) => {
  const { conflictingEntities } = req.body;
  const bucketName = 'entitywatchlistmatchedentities';

  try {
    for (const entity of conflictingEntities) {
      const timestamp = Date.now();
      const fileName = `${entity.text}`;
      const fileContent = JSON.stringify(entity);
      await minioClient.putObject(bucketName, fileName, fileContent, {
        'Content-Type': 'application/json'
      });
    }
    res.status(200).send({ message: 'Conflicting entities saved successfully' });
  } catch (err) {
    console.error('Error saving conflicting entities', err);
    res.status(500).send({ error: 'Failed to save conflicting entities' });
  }
});

app.post('/savetoentitywatchlist', async (req, res) => {
  const bucketName = 'entitywatchlist';
  console.log("req",req)
  try {
    for (const entityName of req.body) {
      const timestamp = Date.now();
      const fileName = `${entityName}`; 
      const fileContent = JSON.stringify({ name: entityName }); 
      await minioClient.putObject(bucketName, fileName, fileContent, {
        'Content-Type': 'application/json'
      });
    }

    res.status(200).send({ message: 'Data saved successfully' });
  } catch (err) {
    console.error('Error saving data', err);
    res.status(500).send({ error: 'Failed to save data' });
  }
});
const storage = multer.diskStorage({
  destination: function (req, file, cb) {
    cb(null, 'uploads/');
  },
  filename: function (req, file, cb) {
    cb(null, file.originalname);
  }
});

// app.post('/detect_faces', async (req, res) => {
//   const { image } = req.body;
//   try {
// const payload = {
//         image: {
//           id: image.id,
//           filename: image.filename,
//           mimetype: image.mimetype,
//           data: image.data
//         }
//       };
//       console.log(image.id)
//       const pythonAPIUrl = 'http://10.176.27.244:5001/detect_faces';
//     const response = await fetch(pythonAPIUrl, {
//       method: 'POST',
//       headers: {
//         'Content-Type': 'application/json'
//       },
//       body: JSON.stringify(payload)
//     });
//     console.log(response)
//     if (response.ok) {
//       const result = await response.json();
      
//       res.json(result);
//     } else {
//       res.status(500).json({ error: 'No response from Python API' });
//     }
//   } catch (error) {
//     console.error('Error:', error);
//     res.status(500).json({ error: 'Internal Server Error' });
//   }
// });

const uploadSingle = multer({ storage: storage }).single('image');

app.post('/faces', uploadSingle, async (req, res) => {
  try {
    const file = req.file;
    console.log(req.body.filename)  //only send this filename to Python API. Then Call http://10.176.26.168:3001/getImageContent/:imageName from Python Code and then return the result
    //    if (!file) {
    //   return res.status(400).json({ error: 'No image uploaded' });
    // }
    // const imageData = fs.readFileSync(file.path, { encoding: 'base64' });
    // const fileId = crypto.randomBytes(16).toString('hex');
    const payload = {
      // image: {
        // id: fileId,
        // filename: file.originalname,
        // mimetype: file.mimetype,
        // data: imageData,
        filename: req.body.filename,
      // }
    };
    const pythonAPIUrl = 'http://10.176.27.244:5001/detect_faces';
    const response = await fetch(pythonAPIUrl, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json'
      },
      body: JSON.stringify(payload)
    });
    if (response.ok) {
      const result = await response.json();
      res.json(result);
    } else {
      res.status(500).json({ error: 'No response from Python API' });
    }
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
});

app.post('/saveentities', async (req, res) => {
  const {actualEntities} = req.body;
  const {objectName} = req.body;
  const bucketName = 'totalentities';

  try {
    for (const entity of actualEntities) {
      const timestamp = Date.now();
      const fileName = `${entity.text}_${timestamp}_${objectName.split(".")[0]}.${objectName.split(".")[1]}`;
      const fileContent = JSON.stringify(entity);
      await minioClient.putObject(bucketName, fileName, fileContent, {
        'Content-Type': 'application/json'
      });
    }

    res.status(200).send({ message: 'Data saved successfully' });
  } catch (err) {
    console.error('Error saving data', err);
    res.status(500).send({ error: 'Failed to save data' });
  }
});

app.post('/start-analysis', async (req, res) => {
  try {
    const analysisEndpoint = 'http://10.176.26.230:5000/perform-analysis';
    const response = await fetch(analysisEndpoint, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ filename: req.body.filename, file_type: req.body.file_type }),
    });
    
    const analysisResult = await response.json();
    res.status(200).json({ analysisResult });
    
    processEntities(analysisResult.result.entities);
  } catch (error) {
    console.error('Error triggering analysis:', error);
    res.status(500).json({ error: 'Failed to start analysis' });
  }
});

async function processEntities(entities) {
  const bucketName = 'clusters';
  const entityBucketName =' totalentities';
  const uniqueLabels = [...new Set(entities.map((entity) => entity.label))];
  for (const label of uniqueLabels) {
    const entitiesWithLabel = entities.filter((entity) => entity.label === label);
    const existingEntitiesJSON = await getEntitiesFromMinio(bucketName, label);
    const mergedEntities = JSON.parse(existingEntitiesJSON).concat(entitiesWithLabel);
    // await putEntitiesToMinio(bucketName, label, mergedEntities);
  }
}

async function getEntitiesFromMinio(bucketName, label) {
  const existingObjectName = `${label}.json`;
  try {
    const existingEntitiesJSON = await new Promise((resolve, reject) => {
      minioClient.getObject(bucketName, existingObjectName, (err, stream) => {
        if (err) {
          if (err.code === 'NoSuchKey') {
            resolve('[]');
          } else {
            console.error(`Error retrieving ${label} entities from MinIO:`, err);
            reject(err);
          }
        } else {
          let existingEntities = '';
          stream.on('data', (chunk) => {
            existingEntities += chunk;
          });
          stream.on('end', () => {
            resolve(existingEntities);
          });
          stream.on('error', (streamErr) => {
            console.error('Stream error:', streamErr);
            reject(streamErr);
          });
        }
      });
    });
    return existingEntitiesJSON;
  } catch (error) {
    console.error(`Error retrieving ${label} entities from MinIO:`, error);
    return '[]';
  }
}

async function putEntitiesToMinio(bucketName, label, entities) {
  const existingObjectName = `${label}.json`;
  try {
    await new Promise((resolve, reject) => {
      minioClient.putObject(bucketName, existingObjectName, JSON.stringify(entities), 'application/json', (err, etag) => {
        if (err) {
          console.error(`Error uploading ${label} entities to MinIO:`, err);
          reject(err);
        } else {
          console.log(`${label} entities saved to MinIO`);
          resolve();
        }
      });
    });
  } catch (error) {
    console.error(`Error uploading ${label} entities to MinIO:`, error);
  }
}


app.get('/getObjectWhere/:objectName', async (req, res) => {
  const { objectName } = req.params;
  const bucketName = "totalentities";
  try {
    const objectsList = await new Promise((resolve, reject) => {
      const collectedObjects = [];

      const objectsListStream = minioClient.listObjects(bucketName, '', true);

      objectsListStream.on('data', obj => {
        collectedObjects.push(obj);
      });

      objectsListStream.on('end', () => {
        resolve(collectedObjects);
      });

      objectsListStream.on('error', err => {
        reject(err);
      });
    });

    const matchingObjects = objectsList.filter(obj => {
      const parts = obj.name.split('_');
      if (parts.length > 3) {
        // Extract the part after the second last "_"
        const filename = parts.slice(-2).join('_');
    
        // console.log("extractedFileName", filename);
    
        return objectName === filename;
      }
      const filename = parts[2];

      // console.log("extractedFileName", filename);
    
      return objectName === filename;
    });

    if (matchingObjects.length === 0) {
      return res.status(404).json({ error: 'File not found' });
    }
    matchingObjects.forEach(matchingObject => {
    });

    // Return the matching objects details
    return res.status(200).json(matchingObjects.map(matchingObject => ({
      name: matchingObject.name,
      lastModified: matchingObject.lastModified,
      size: matchingObject.size
    })));
  } catch (error) {
    console.error('Error listing objects:', error);
    res.status(500).json({ error: 'Error listing objects' });
  }
});



app.get('/getObject/:objectName', (req, res) => {
  const { objectName } = req.params;
  const bucketName = "totalentities"
  minioClient.getObject(bucketName, objectName, (err, dataStream) => {
    if (err) {
      console.error('Error retrieving object:', err);
      res.status(500).json({ error: 'Error retrieving object' });
    } else {
      let objectContent = '';
      dataStream.on('data', (chunk) => {
        objectContent += chunk;
      });
      dataStream.on('end', () => {
        res.json({ content: objectContent });
      });

      dataStream.on('error', (readErr) => {
        console.error('Error reading object data:', readErr);
        res.status(500).json({ error: 'Error reading object data' });
      });
    }
  });
});

app.get('/getAllEntities', (req, res) => {
  const bucketName = 'totalentities'; 

  const objectsList = [];

  const objectsStream = minioClient.listObjectsV2(bucketName, '', true);

  objectsStream.on('data', obj => {
    objectsList.push({
      name: obj.name,
      size: obj.size,
      lastModified: obj.lastModified,
    }
    
    );
  });

  objectsStream.on('error', err => {
    console.log('Error listing objects: ', err);
    res.status(500).json({ error: 'Error fetching MinIO data' });
  });

  objectsStream.on('end', () => {
    console.log('Listing objects finished.');
    res.json(objectsList);
  });
});

app.get('/fetchmetadata/:objectName', (req, res) => {
  const bucketName = 'metadata';
  const objectName = req.params.objectName + "_metadata";
  minioClient.getObject(bucketName, objectName, (err, stream) => {
    if (err) {
      console.error('Error retrieving metadata from MinIO:', err);
      return res.status(500).json({ error: 'Error retrieving metadata from MinIO' });
    }

    let metadata = '';
    stream.on('data', (chunk) => {
      metadata += chunk;
    });

    stream.on('end', () => {
      try {
        const parsedMetadata = JSON.parse(metadata);
        res.json(parsedMetadata);
      } catch (parseError) {
        console.error('Error parsing metadata:', parseError);
        return res.status(500).json({ error: 'Error parsing metadata' });
      }
    });

    stream.on('error', (streamErr) => {
      console.error('Stream error:', streamErr);
      res.status(500).json({ error: 'Stream error' });
    });
  });
});
app.get('/getLastModified', async (req, res) => {
  const bucketName = 'files';
  const objectsList = [];

  try {
    const objectsStream = minioClient.listObjectsV2(bucketName, '', true);

    const getObjectPromises = [];

    for await (const obj of objectsStream) {
      const metadata = {
        name: obj.name,
        size: obj.size,
        lastModified: obj.lastModified,
      };

      const getObjectPromise = new Promise((resolve, reject) => {
        const chunks = [];

        minioClient.getObject(bucketName, obj.name, (err, dataStream) => {
          if (err) {
            reject(err);
            return;
          }

          dataStream.on('data', chunk => {
            chunks.push(chunk);
          });

          dataStream.on('end', () => {
            metadata.content = Buffer.concat(chunks).toString('utf-8');
            objectsList.push(metadata);
            resolve();
          });

          dataStream.on('error', err => {
            console.log('Error reading object stream: ', err);
            reject(err);
          });
        });
      });

      getObjectPromises.push(getObjectPromise);
    }

    // Wait for all getObject promises to complete
    await Promise.all(getObjectPromises);

    console.log('Listing objects finished.');
    res.json(objectsList);
  } catch (err) {
    console.log('Error listing or getting objects: ', err);
    res.status(500).json({ error: 'Error fetching MinIO data' });
  }
});
app.get('/getEntities', async (req, res) => {
  const bucketName = 'clusters';
  const objectsList = [];
  try {
    const objectsStream = minioClient.listObjectsV2(bucketName, '', true);
    const getObjectPromises = [];
    for await (const obj of objectsStream) {
      const metadata = {
        name: obj.name,
        size: obj.size,
        lastModified: obj.lastModified,
      };

      const getObjectPromise = new Promise((resolve, reject) => {
        const chunks = [];

        minioClient.getObject(bucketName, obj.name, (err, dataStream) => {
          if (err) {
            reject(err);
            return;
          }

          dataStream.on('data', chunk => {
            chunks.push(chunk);
          });

          dataStream.on('end', () => {
            metadata.content = Buffer.concat(chunks).toString('utf-8');
            objectsList.push(metadata);
            resolve();
          });

          dataStream.on('error', err => {
            console.log('Error reading object stream: ', err);
            reject(err);
          });
        });
      });

      getObjectPromises.push(getObjectPromise);
    }

    // Wait for all getObject promises to complete
    await Promise.all(getObjectPromises);

    console.log('Listing objects finished.');
    res.json(objectsList);
  } catch (err) {
    console.log('Error listing or getting objects: ', err);
    res.status(500).json({ error: 'Error fetching MinIO data' });
  }
});

// Function to generate random source values
const getRandomSource = () => {
  const sources = ['Local', 'External', 'Tools']; // Define your source options here
  const randomIndex = Math.floor(Math.random() * sources.length);
  return sources[randomIndex];
};

// Endpoint to fetch all metadata
app.get('/fetchAllMetadata', (req, res) => {
  const bucketName = 'metadata'; 
  const objectsList = [];

  const objectsStream = minioClient.listObjectsV2(bucketName, '', true);
  
  objectsStream.on('data', obj => {
    const source = getRandomSource(); // Get a random source for each object
    objectsList.push({
      name: obj.name,
      size: obj.size,
      lastModified: obj.lastModified,
      source: source, // Add the randomly generated source to each object
    });
  });

  objectsStream.on('error', err => {
    console.log('Error listing objects: ', err);
    res.status(500).json({ error: 'Error fetching MinIO data' });
  });

  objectsStream.on('end', () => {
    console.log('Listing objects finished.');
    res.json(objectsList);
  });
});

// if you want to list all the different versions of an object use this code

// app.get('/getMinioData', (req, res) => {
//   const bucketName = 'files'; 
//   const objectsList = [];
//   const objectsStream = minioClient.listObjects(bucketName, '', true, { IncludeVersion: true });
//   objectsStream.on('data', obj => {
//     objectsList.push({
//       name: obj.name,
//       size: obj.size,
//       lastModified: obj.lastModified,
//       eTag: obj.etag,
//       versionId: obj.versionId
//     });
//   });

//   objectsStream.on('error', err => {
//     console.log('Error listing objects: ', err);
//     res.status(500).json({ error: 'Error fetching MinIO data' });
//   });

//   objectsStream.on('end', () => {
//     console.log('Listing objects finished.');
//     res.json(objectsList);
//     console.log("objectList",objectsList)
//   });
// });

app.get('/getMinioImageData', (req, res) => {
  const bucketName = 'images'; 
  const objectsList = [];
  const objectsStream = minioClient.listObjectsV2(bucketName, '', true, '', { IncludeVersion: true });
  objectsStream.on('data', obj => {
    objectsList.push({
      name: obj.name,
      // size: obj.size,
      // lastModified: obj.lastModified,
      // eTag: obj.etag,
      // versionId: obj.versionId
    });
  });

  objectsStream.on('error', err => {
    console.log('Error listing objects: ', err);
    res.status(500).json({ error: 'Error fetching MinIO data' });
  });

  objectsStream.on('end', () => {
    console.log('Listing objects finished.');
    res.json(objectsList);
  });
});

app.get('/getMinioData', (req, res) => {
  const bucketName = 'files'; 
  const objectsList = [];
  const objectsStream = minioClient.listObjectsV2(bucketName, '', true, '', { IncludeVersion: true });
  objectsStream.on('data', obj => {
    objectsList.push({
      name: obj.name,
      size: obj.size,
      lastModified: obj.lastModified,
      eTag: obj.etag,
      versionId: obj.versionId
    });
  });

  objectsStream.on('error', err => {
    console.log('Error listing objects: ', err);
    res.status(500).json({ error: 'Error fetching MinIO data' });
  });

  objectsStream.on('end', () => {
    console.log('Listing objects finished.');
    res.json(objectsList);
  });
});

// delete_image
app.delete('/delete_image/:objectName', (req, res) => {
  const bucketName = 'images';
  const objectName = req.params.objectName;
console.log("objectName",objectName)
  minioClient.removeObject(bucketName, objectName, (err) => {
    if (err) {
      console.error('Error deleting file from MinIO:', err);
      return res.status(500).json({ error: 'Error deleting file from MinIO' });
    }

    res.json({ message: 'File deleted successfully' });
  });
});

app.delete('/delete/:objectName', (req, res) => {
  const bucketName = 'files';
  const objectName = req.params.objectName;
console.log("objectName",objectName)
  minioClient.removeObject(bucketName, objectName, (err) => {
    if (err) {
      console.error('Error deleting file from MinIO:', err);
      return res.status(500).json({ error: 'Error deleting file from MinIO' });
    }

    res.json({ message: 'File deleted successfully' });
  });
});

// getFileName
// app.get('/getFileName/:objectName', (req, res) => {
//   console.log("REQ", req.params.objectName)
//   const bucketName = 'files';
//   const objectName = req.params.objectName;

//   minioClient.getObject(bucketName, objectName, (err, dataStream) => {
//     if (err) {
//       console.log('Error retrieving file from MinIO:', err);
//       return res.status(500).json({ error: 'Error fetching file from MinIO' });
//     }

//     let fileData = '';

//     dataStream.on('data', chunk => {
//       fileData += chunk;
//     });

//     dataStream.on('end', () => {
//       res.send(fileData); // Send the file data as response
//     });

//     dataStream.on('error', err => {
//       console.log('Error reading file data:', err);
//       res.status(500).json({ error: 'Error reading file data' });
//     });
//   });
// });

app.get('/getImageContent/:imageName', async (req, res) => {
  try {
    const bucketName = 'images';
    const imageName = req.params.imageName;
    const imageData = await minioClient.getObject(bucketName, imageName);
    let imageDataBuffer = Buffer.from('');
    imageData.on('data', (chunk) => {
      imageDataBuffer = Buffer.concat([imageDataBuffer, chunk]);
    });

    imageData.on('end', () => {
      const base64ImageData = imageDataBuffer.toString('base64');
      const imageDataURI = `data:image/jpeg;base64,${base64ImageData}`;
      res.setHeader('Content-Type', 'text/plain');
      res.send(imageDataURI);
    });
  } catch (error) {
    console.error('Error fetching image:', error);
    res.status(500).send('Error fetching image');
  }
});


app.get('/getObjectContent/:objectName', (req, res) => {
  const bucketName = 'files';
  const objectName = req.params.objectName;
  minioClient.getObject(bucketName, objectName, (err, dataStream) => {
    if (err) {
      console.log('Error retrieving file from MinIO:', err);
      return res.status(500).json({ error: 'Error fetching file from MinIO' });
    }

    const chunks = [];

    dataStream.on('data', chunk => {
      chunks.push(chunk);
    });

    dataStream.on('end', () => {
      const fileData = Buffer.concat(chunks);
      console.log(fileData)
      res.send(fileData); 
    });

    dataStream.on('error', err => {
      console.log('Error reading file data:', err);
      res.status(500).json({ error: 'Error reading file data' });
    });
  });
});

// search_image

app.get('/search_image/:objectName', (req, res) => {
  const { objectName } = req.params;
  const bucketName = 'images';

  minioClient.statObject(bucketName, objectName, (err, stat) => {
    if (err) {
      console.error('Error fetching file from MinIO:', err);
      return res.status(500).json({ error: 'Error fetching file' });
    }

    const etag = stat.etag;
    if (!etag) {
      return res.status(500).json({ error: 'ETag not found for the requested image' });
    }

    // const imagesWithSameETag = [];

    // // List objects in the bucket
    // minioClient.listObjects(bucketName, '', true)
    //   .on('data', obj => {
    //     if (obj.etag === etag && obj.name !== objectName) {
    //       imagesWithSameETag.push(obj.name);
    //     }
    //   })
    //   .on('error', err => {
    //     console.error('Error reading objects from stream:', err);
    //     res.status(500).json({ error: 'Error reading objects' });
    //   })
    //   .on('end', () => {
    //     res.status(200).json({ imagesWithSameETag });
    //   });

    const axios = require('axios');
    const crypto = require('crypto');
    
    const objectsStream = minioClient.listObjects('images', '', true);
    const images = [];
    let processedCount = 0;
    let totalCount = 0;
    
    objectsStream.on('data', async obj => {
      totalCount++;
      try {
        const response = await axios.get(`http://172.17.0.2:9000/images/${obj.name}`, { responseType: 'arraybuffer' });
        const imageData = Buffer.from(response.data).toString('base64');
        const fileId = crypto.randomBytes(16).toString('hex');
        const payload = {
          image: {
            id: fileId,
            filename: obj.name,
            mimetype: 'image/jpeg',
            data: imageData
          }
        };
        if (obj.etag === etag && obj.name.split("-")[0] !== objectName) {
                images.push(payload);
               }
        processedCount++; 
        checkCompletion();
      } catch (error) {
        console.error('Error fetching image:', error);
      }
    });
    
    function checkCompletion() {
      if (processedCount === totalCount) {
        res.json(images);
      }
    }
    
  });
  
});


app.get('/download_image/:objectName', (req, res) => {
  const { objectName } = req.params;
  
  const bucketName = 'images';

  minioClient.getObject(bucketName, objectName, (err, dataStream) => {
    if (err) {
      console.error('Error fetching file from MinIO:', err);
      return res.status(500).json({ error: 'Error fetching file' });
    }

    res.attachment(objectName); // Set the filename in the download header
    dataStream.pipe(res); // Pipe the data stream to the response
  });
});
app.get('/download/:objectName', (req, res) => {
  const { objectName } = req.params;
  
  const bucketName = 'files';

  minioClient.getObject(bucketName, objectName, (err, dataStream) => {
    if (err) {
      console.error('Error fetching file from MinIO:', err);
      return res.status(500).json({ error: 'Error fetching file' });
    }

    res.attachment(objectName); // Set the filename in the download header
    dataStream.pipe(res); // Pipe the data stream to the response
  });
});

const isImage = (mimetype) => {
  return mimetype.startsWith('image/');
};

app.post('/upload', upload.single('file'), async (req, res) => {
  if (!req.file) {
    return res.status(400).json({ error: 'No file uploaded' });
  }
  
  const file = req.file;
  console.log("file",req.body)
  if (isImage(file.mimetype)) {
    req.originalUrl = '/upload-image';
    return uploadImageHandler(req, res);
  }

  const metadataBucket = 'metadata';
  const hash = crypto.createHash('sha256');
  const fileStream = fs.createReadStream(file.path);
  const originalName = req.file.originalname;
  const bucketName = 'files';
  const uploadTimestamp = new Date().toISOString();
  const fileHash = await calculateFileHash(fileStream, hash);
  try {
    await uploadFileToMinio(bucketName, originalName, file.path, uploadTimestamp);
    await uploadMetadataToMinio(metadataBucket, originalName, fileHash, uploadTimestamp, file.mimetype);
    res.json({ message: 'File uploaded successfully with metadata' });
  } catch (error) {
    console.error('Error uploading file:', error);
    res.status(500).json({ error: 'Error uploading file' });
  }
});

let fileNames = [];
let fileEtags = [];
const uploadImageHandler = async (req, res) => {
  try {
    console.log("inside uploadImageHandler")
    const file = req.file;
    if (!file) {
      return res.status(400).json({ error: 'No image uploaded' });
    }

    const objectName = file.originalname;
    const fileStream = fs.createReadStream(file.path);
    const uploadPromise = new Promise((resolve, reject) => {
      minioClient.putObject('images', objectName, fileStream, (err, etag) => {
        if (err) {
          reject(err);
        } else {
          resolve(etag);
        }
      });
    });
    const etag = await uploadPromise
    fs.unlinkSync(file.path);
    
    res.json({ 
      message: 'Image uploaded successfully', 
      type: file.mimetype,
    });

    minioClient.statObject('images', objectName, async (err, stat) => {
      if (err) {
        console.error('Error fetching file from MinIO:', err);
        return;
      }

      const fileEtag = stat.etag;


      fileNames.push(objectName);
      fileEtags.push(fileEtag);

      if (parseInt(req.body.totalFiles) === fileNames.length) {
        console.log("All files uploaded, proceeding...");

        try {
          const payload = {
            upload_status: 'completed',
            fileName: fileNames,
            fileEtag: fileEtags
          };

          const pythonApiResponse = await fetch('http://10.176.27.244:5001/start_face_indexing', {
            method: 'POST',
            body: JSON.stringify(payload),
            headers: {
              'Content-Type': 'application/json'
            }
          });

          if (!pythonApiResponse.ok) {
            throw new Error('Failed to send status to Python API');
          }

          const responseData = await pythonApiResponse.json();
          console.log('Response from Python API:', responseData);
          console.log('Status sent to Python API:', 'completed');
        } catch (error) {
          console.error('Error sending status to Python API:', error);
        } finally {
          fileNames = [];
          fileEtags = [];
        }
      }
    });
  } catch (error) {
    console.error('Error:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }
};




let value = 0;
let interval;

let indexingStatus = 0;

const updateIndexingStatus = async () => {
  
};


app.get('/getIndexingStatus', async (req, res) => {
  try {
    const response = await fetch('http://10.176.27.244:5001/index_status');
    const data = await response.json();
    console.log("data", data);
    indexingStatus = data.index_status;
  } catch (error) {
    console.error("Error fetching indexing status:", error);
  }
  res.json({ indexingStatus });
});



app.get('/images', async (req, res) => {
  const axios = require('axios');
  const crypto = require('crypto');
  const { PassThrough } = require('stream');
try {
  const objectsStream = minioClient.listObjectsV2('images', '', true);
  let images = [];
  let processedCount = 0;
  let totalCount = 0;

    for await (const obj of objectsStream) {
      const response = await axios.get(`http://172.17.0.2:9000/images/${obj.name}`, { responseType: 'stream' });
      const imageDataStream = response.data;
      const imageDataBuffer = await streamToBuffer(imageDataStream);
      const imageDataBase64 = imageDataBuffer.toString('base64');

      const payload = {
        image: {
          id: crypto.randomBytes(16).toString('hex'),
          lastModified: obj.lastModified,
          size: obj.size,
          filename: obj.name,
          mimetype: 'image/jpeg',
          data: imageDataBase64
        }
      };
      images.push(payload);
    }

    res.json(images);
  
  } catch (error) {
    console.error('Error fetching images:', error);
    res.status(500).json({ error: 'Internal Server Error' });
  }

  function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', chunk => chunks.push(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
  }

  function streamToBuffer(stream) {
    return new Promise((resolve, reject) => {
      const chunks = [];
      stream.on('data', chunk => chunks.push(chunk));
      stream.on('error', reject);
      stream.on('end', () => resolve(Buffer.concat(chunks)));
    });
  }
});




// app.get('/imagesnew', async (req, res) => {
//   const axios = require('axios');
//   const crypto = require('crypto');
  
//   const objectsStream = minioClient.listObjects('imagesnew', '', true);
//   const images = [];
  
//   // Promise to wait for all requests to complete
//   const requests = [];
  
//   objectsStream.on('data', async obj => {
//     try {
//       const request = axios.get(`http://172.17.0.2:9000/imagesnew/${obj.name}`, { responseType: 'arraybuffer' })
//         .then(response => {
//           const imageData = Buffer.from(response.data).toString('base64');
//           const fileId = crypto.randomBytes(16).toString('hex');
//           const payload = {
//             image: {
//               id: fileId,
//               filename: obj.name,
//               mimetype: 'image/jpeg', // Adjust mimetype if necessary
//               data: imageData
//             }
//           };
//           images.push(payload);
//         })
//         .catch(error => {
//           console.error('Error fetching image:', error);
//         });
      
//       requests.push(request);
//     } catch (error) {
//       console.error('Error processing image:', error);
//     }
//   });
  
//   // Wait for all requests to complete before sending the response
//   objectsStream.on('end', () => {
//     Promise.all(requests)
//       .then(() => {
//         res.json(images);
//       })
//       .catch(error => {
//         console.error('Error sending response:', error);
//         res.status(500).json({ error: 'Internal Server Error' });
//       });
//   });
// });



async function calculateFileHash(stream, hash) {
  return new Promise((resolve, reject) => {
    stream.on('data', data => hash.update(data));
    stream.on('end', () => {
      resolve(hash.digest('hex'));
    });
    stream.on('error', error => reject(error));
  });
}

async function uploadFileToMinio(bucketName, fileName, filePath, uploadTimestamp) {
  
  return new Promise((resolve, reject) => {
    fs.stat(filePath, (err, stats) => {
      if (err) {
        reject(err);
        return;
      }
      const metadata = {
        'uploadDate': uploadTimestamp,
        // 'fileSize': stats.size 
      };

      minioClient.fPutObject(bucketName, fileName, filePath, metadata, (err, etag) => {     
        if (err) {
          reject(err);
        } else {
          resolve(etag);
        }
      });
    });
  });
}

async function uploadMetadataToMinio(metadataBucket, fileName, fileHash, uploadTimestamp, mimeType) {
  return new Promise((resolve, reject) => {
    const metadataObjectName = `${fileName}_metadata`;
    const metadata = {
      'uploadDate': uploadTimestamp,
      'metaHash': fileHash,
      'source': 'local',
    };
    if (mimeType === 'text/plain') {
      metadata.tag = 'text';
    }
    minioClient.putObject(metadataBucket, metadataObjectName, JSON.stringify(metadata), (err) => {
      if (err) {
        reject(err);
      } else {
        resolve();
      }
    });
  });
} 


app.listen(PORT, '0.0.0.0', () => {
  console.log(`Server running on port ${PORT}`);
});
