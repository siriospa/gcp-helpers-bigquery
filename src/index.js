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

/**
 * Gets or create a dataset.
 *
 * @param {object} client BigQuery client.
 * @param {string} name ID or name.
 * @returns The dataset with the given name.
 */
exports.dataset = async (client, name) => {
  const dataset = client.dataset(name)
  const [status] = await dataset.exists()

  if (!status) {
    return await dataset.create()
  } else {
    return await dataset.get()
  }
}

/**
 * Gets or create a table.
 *
 * @param {object} dataset BigQuery dataset.
 * @param {string} name ID or name.
 * @param {object} options
 * @returns The table with the given name.
 */
exports.table = async (dataset, name, options) => {
  const table = dataset.table(name)
  const [status] = await table.exists()

  if (!status) {
    return await table.create(options)
  } else {
    return await table.get()
  }
}
