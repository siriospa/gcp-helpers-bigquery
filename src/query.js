// Copyright 2024 Sirio S.p.A.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const fs = require("fs")
const { logErrors } = require("./error")

/**
 * Returns the SQL script related to the given name.
 *
 * @param {object} filepath Path of the SQL file.
 * @param {BigQuery} client BigQuery client.
 * @returns {boolean} A value indicating whether the SQL script has been executed.
 */
exports.executeSqlFile = async (filepath, client, params) => {
  const query = this.readSqlFile(filepath)

  if (query) {
    const [job] = await client.createQueryJob({
      query,
      params,
    })

    const results = await job.getQueryResults().catch(logErrors)
    let done = false

    results.forEach(({ jobComplete }) => {
      if (typeof jobComplete !== "undefined" && jobComplete === true) {
        done = true
      }
    })

    return done
  }
  return false
}

/**
 * Determines whether the given SQL script file exists.
 *
 * @param {string} filepath Path of the SQL file.
 * @returns {boolean} A value indicating whether the given SQL script file exists.
 */
exports.existsSqlFile = (filepath) => this.readSqlFile(filepath) !== null

/**
 * Reads the file data.
 *
 * @param {string} filepath Path of the SQL file.
 * @returns {string} SQL file data.
 */
exports.readSqlFile = (filepath) => {
  if (fs.existsSync(filepath)) {
    const file = require.resolve(filepath)

    return fs.readFileSync(file, "utf8")
  }

  return null
}
