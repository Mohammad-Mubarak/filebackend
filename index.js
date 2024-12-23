const express = require('express');
const cors = require('cors');
const { faker } = require('@faker-js/faker');
const { v4: uuidv4 } = require('uuid');

const app = express();
const PORT = process.env.PORT || 5000;

app.use(cors());
app.use(express.json());

const educationLevels = [
  "High School",
  "Associate Degree",
  "Bachelor's Degree",
  "Master's Degree",
  "Doctorate",
  "Ph D",
  "Professional Certification"
];

const getRandomEducationLevel = () => {
  const randomIndex = Math.floor(Math.random() * educationLevels.length);
  return educationLevels[randomIndex];
};

const estimateRecordCount = (fileSizeMB, averageRecordSize = 200) => {
  const bytes = fileSizeMB * 1024 * 1024;
  return Math.floor(bytes / averageRecordSize);
};


const generateField = (prop) => {
  const propNameLower = prop.name.toLowerCase();
  // Accurate regex with word boundaries
  if (/\bname\b/i.test(propNameLower)) {
    return faker.person.fullName();
  } else if (/\bemail\b/i.test(propNameLower)) {
    return faker.internet.email();
  } else if (/\education\b/i.test(propNameLower)) {
    return getRandomEducationLevel();
  } else if (/\b(phone|contact)\b/i.test(propNameLower)) {
    return faker.phone.number();
  } else if (/\bgender\b/i.test(propNameLower)) {
    return faker.person.sexType();
  } else if (/\bcity\b/i.test(propNameLower)) {
    return faker.location.city();
  } else if (/\bstate\b/i.test(propNameLower)) {
    return faker.location.state();
  } else if (/\baddress\b/i.test(propNameLower)) {
    return faker.location.streetAddress();
  } else if (/\bcountry\b/i.test(propNameLower)) {
    return faker.location.country();
  } else if (/\b(zip|postal)\b/i.test(propNameLower)) {
    return faker.location.zipCode();
  } else if (/\bcompany\b/i.test(propNameLower)) {
    return faker.company.name();
  } else if (/\b(date|dob)\b/i.test(propNameLower)) {
    return faker.date.birthdate();
  } else if (/\b(id|uuid)\b/i.test(propNameLower)) {
    return faker.string.uuid();
  } else if (/\b(salary|income)\b/i.test(propNameLower)) {
    return faker.finance.amount(30000, 150000, 2, '$');
  } else if (/\bage\b/i.test(propNameLower)) {
    return faker.number.int({ min: 18, max: 80 });
  } else if (/\b(description|bio)\b/i.test(propNameLower)) {
    return faker.lorem.sentences(2);
  } else if (/\b(url|website)\b/i.test(propNameLower)) {
    return faker.internet.url();
  } else if (/\b(image|avatar)\b/i.test(propNameLower)) {
    return faker.image.avatar();
  } else {
    switch (prop.type) {
      case 'string':
        return faker.lorem.word();
      case 'number':
        return faker.datatype.number();
      case 'boolean':
        return faker.datatype.boolean();
      case 'date':
        return faker.date.past().toISOString();
      case 'address':
        return faker.location.streetAddress();
      case 'uuid':
        return faker.string.uuid();
      default:
        return '';
    }
  }
};

  
const generateRecord = (properties) => {
    const record = {};
  
    properties.forEach((prop) => {
      record[prop.name] = generateField(prop);
    });
  
    // Handle primary key generation
    const primaryKeyProp = properties.find((prop) => prop.primaryKey);
    if (primaryKeyProp) {
      if (primaryKeyProp.type === 'number') {
        record[primaryKeyProp.name] = faker.datatype.number({ min: 10, max: 1000000000000000000 });
      } else if (primaryKeyProp.type === 'uuid' || primaryKeyProp.type === 'string') {
        record[primaryKeyProp.name] = uuidv4();
      }
    }
    return record;
  };
  
// Corrected Streaming CSV Function
const streamCSV = (properties, recordCount, res) => {
  res.setHeader('Content-Disposition', 'attachment; filename="generated_file.csv"');
  res.setHeader('Content-Type', 'text/csv');

  // Write CSV headers
  const headers = properties.map(prop => prop.name).join(',') + '\n';
  res.write(headers);

  let sentRecords = 0;

  const generate = () => {
    let ok = true;
    while (sentRecords < recordCount && ok) {
      const record = generateRecord(properties);
      
      // Escape and format each field
      const csvLine = properties.map(prop => {
        let value = record[prop.name];
        if (typeof value === 'string') {
          // Escape double quotes by doubling them
          value = value.replace(/"/g, '""');
          // If value contains comma, newline, or double quotes, wrap it in double quotes
          if (value.search(/("|,|\n)/g) >= 0) {
            value = `"${value}"`;
          }
        }
        return value;
      }).join(',') + '\n';

      sentRecords += 1;
      ok = res.write(csvLine);

      // Flush every 1000 records to manage backpressure
      if (sentRecords % 1000 === 0) {
        res.flush && res.flush();
      }
    }

    if (sentRecords < recordCount) {
      // If not finished, wait for drain event before continuing
      res.once('drain', generate);
    } else {
      // End the response once all records are sent
      res.end();
      console.log(`Finished generating ${sentRecords} records.`);
    }
  };

  generate();
};

// Streaming JSON and XML functions remain unchanged
const streamJSON = (properties, recordCount, res) => {
  res.setHeader('Content-Disposition', 'attachment; filename="generated_file.json"');
  res.setHeader('Content-Type', 'application/json');

  let sentRecords = 0;
  res.write('[');

  const generate = () => {
    let ok = true;
    while (sentRecords < recordCount && ok) {
      const record = generateRecord(properties);
      const jsonLine = JSON.stringify(record);
      sentRecords += 1;
      ok = res.write(jsonLine + (sentRecords < recordCount ? ',' : ''));
      if (sentRecords % 1000 === 0) {
        res.flush && res.flush();
      }
    }
    if (sentRecords < recordCount) {
      res.once('drain', generate);
    } else {
      res.write(']');
      res.end();
      console.log(`Finished generating ${sentRecords} records.`);
    }
  };

  generate();
};

const streamXML = (properties, recordCount, res) => {
  res.setHeader('Content-Disposition', 'attachment; filename="generated_file.xml"');
  res.setHeader('Content-Type', 'application/xml');

  res.write('<?xml version="1.0" encoding="UTF-8"?>\n<records>\n');

  let sentRecords = 0;

  const generate = () => {
    let ok = true;
    while (sentRecords < recordCount && ok) {
      const record = generateRecord(properties);
      let xmlElement = '  <record>\n';
      properties.forEach((prop) => {
        xmlElement += `    <${prop.name}>${record[prop.name]}</${prop.name}>\n`;
      });
      xmlElement += '  </record>\n';
      sentRecords += 1;
      ok = res.write(xmlElement);
      if (sentRecords % 1000 === 0) {
        res.flush && res.flush();
      }
    }
    if (sentRecords < recordCount) {
      res.once('drain', generate);
    } else {
      res.write('</records>');
      res.end();
      console.log(`Finished generating ${sentRecords} records.`);
    }
  };

  generate();
};

app.post('/api/generate', async (req, res) => {
  try {
    const { fileType, fileSize, properties } = req.body;
    console.log(req.body);

    if (!['json', 'csv', 'xml'].includes(fileType)) {
      return res.status(400).json({ error: 'Invalid fileType. Allowed types: json, csv, xml.' });
    }

    const maxFileSizeMB = 1000;
    if (
      typeof fileSize !== 'number' ||
      isNaN(fileSize) ||
      fileSize < 1 ||
      fileSize > maxFileSizeMB
    ) {
      return res
        .status(400)
        .json({ error: `fileSize must be a number between 1 and ${maxFileSizeMB} MB.` });
    }

    if (!Array.isArray(properties) || properties.length === 0) {
      return res.status(400).json({ error: 'properties must be a non-empty array.' });
    }

    const primaryKeys = properties.filter((prop) => prop.primaryKey);
    if (primaryKeys.length > 1) {
      return res.status(400).json({ error: 'Only one property can be marked as primaryKey.' });
    }

    if (primaryKeys.length === 0) {
      return res.status(400).json({ error: 'At least one property must be marked as primaryKey.' });
    }

    const validTypes = ['string', 'number', 'boolean', 'date', 'email', 'phone', 'address', 'uuid'];
    for (const prop of properties) {
      if (
        typeof prop.name !== 'string' ||
        prop.name.trim() === '' ||
        !validTypes.includes(prop.type)
      ) {
        return res.status(400).json({
          error: `Each property must have a valid 'name' (non-empty string) and 'type' (${validTypes.join(
            ', '
          )}).`,
        });
      }
    }

    let averageRecordSize = 200;
    switch (fileType) {
      case 'csv':
        averageRecordSize = 100;
        break;
      case 'xml':
        averageRecordSize = 300;
        break;
      default:
        averageRecordSize = 200;
    }

    const estimatedRecords = estimateRecordCount(fileSize, averageRecordSize);

    switch (fileType) {
      case 'json':
        streamJSON(properties, estimatedRecords, res);
        break;
      case 'csv':
        streamCSV(properties, estimatedRecords, res);
        break;
      case 'xml':
        streamXML(properties, estimatedRecords, res);
        break;
      default:
        res.status(400).json({ error: 'Unsupported fileType.' });
    }
  } catch (error) {
    console.error(error);
    res.status(500).json({ error: 'Internal Server Error.' });
  }
});

app.get("/test",(req,res)=>{
   res.json({message:"Hello World! This is md"});
})

app.listen(PORT, () => {
  console.log(`Server is running on port ${PORT}`);
});
