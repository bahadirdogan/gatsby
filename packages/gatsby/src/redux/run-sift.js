// @flow
const { default: sift } = require(`sift`)
const _ = require(`lodash`)
const prepareRegex = require(`../utils/prepare-regex`)
const { makeRe } = require(`micromatch`)
const { getValueAt } = require(`../utils/get-value-at`)
const {
  toDottedFields,
  objectToDottedField,
  liftResolvedFields,
} = require(`../db/common/query`)
const {
  ensureIndexByTypedChain,
  getNodesByTypedChain,
  addResolvedNodes,
  getNode,
} = require(`./nodes`)

/////////////////////////////////////////////////////////////////////
// Parse filter
/////////////////////////////////////////////////////////////////////

const prepareQueryArgs = (filterFields = {}) =>
  Object.keys(filterFields).reduce((acc, key) => {
    const value = filterFields[key]
    if (_.isPlainObject(value)) {
      acc[key === `elemMatch` ? `$elemMatch` : key] = prepareQueryArgs(value)
    } else {
      switch (key) {
        case `regex`:
          acc[`$regex`] = prepareRegex(value)
          break
        case `glob`:
          acc[`$regex`] = makeRe(value)
          break
        default:
          acc[`$${key}`] = value
      }
    }
    return acc
  }, {})

const getFilters = filters =>
  Object.keys(filters).reduce(
    (acc, key) => acc.push({ [key]: filters[key] }) && acc,
    []
  )

/////////////////////////////////////////////////////////////////////
// Run Sift
/////////////////////////////////////////////////////////////////////

function isEqId(siftArgs) {
  // The `id` of each node is invariably unique. So if a query is doing id $eq(string) it can find only one node tops
  return (
    siftArgs.length > 0 &&
    siftArgs[0].id &&
    Object.keys(siftArgs[0].id).length === 1 &&
    Object.keys(siftArgs[0].id)[0] === `$eq`
  )
}

function handleFirst(siftArgs, nodes) {
  if (nodes.length === 0) {
    return []
  }

  const index = _.isEmpty(siftArgs)
    ? 0
    : nodes.findIndex(
        sift({
          $and: siftArgs,
        })
      )

  if (index !== -1) {
    return [nodes[index]]
  } else {
    return []
  }
}

function handleMany(siftArgs, nodes) {
  let result = _.isEmpty(siftArgs)
    ? nodes
    : nodes.filter(
        sift({
          $and: siftArgs,
        })
      )

  if (!result || !result.length) {
    return null
  }

  return result
}

/**
 * Given an object, assert that it has exactly one leaf property and that this
 * leaf is a number, string, or boolean. Additionally confirms that the path
 * does not contain the special cased `elemMatch` name.
 * Returns undefined if not a flat path, if it contains `elemMatch`, or if the
 * leaf value was not a bool, number, or string.
 * If array, it contains the property path followed by the leaf value.
 *
 * Example: `{a: {b: {c: "x"}}}` is flat with a chain of `['a', 'b', 'c', 'x']`
 * Example: `{a: {b: "x", c: "y"}}` is not flat because x and y are 2 leafs
 *
 * @param {Object} obj
 * @returns {Array<string | number | boolean>|undefined}
 */
const getFlatPropertyChain = obj => {
  if (!obj) {
    return undefined
  }

  let chain = []
  let props = Object.getOwnPropertyNames(obj)
  let next = obj
  while (props.length === 1) {
    const prop = props[0]
    if (prop === `elemMatch`) {
      // TODO: Support handling this special case without sift as well
      return undefined
    }
    chain.push(prop)
    next = next[prop]
    if (
      typeof next === `string` ||
      typeof next === `number` ||
      typeof next === `boolean`
    ) {
      // Add to chain so we can return it
      chain.push(next)
      return chain
    }
    if (!next) {
      // Must be null or undefined since we checked the rest above
      return undefined
    }
    props = Object.getOwnPropertyNames(next)
  }

  // This means at least one object in the chain had more than one property
  return undefined
}

/**
 * Given the chain of a simple filter, return the set of nodes that pass the
 * filter. The chain should be a property chain leading to the property to
 * check, followed by the value to check against.
 * Only nodes of given node types will be considered (a fast index is created
 * if one doesn't exist).
 * The empty result value is null if firstOnly is false, or else an empty array.
 *
 * @param {Array<string>} chain
 * @param {boolean|number|string} targetValue
 * @param {Array<string>} nodeTypeNames
 * @returns {Object[]|null}
 */
const runFlatFilterWithoutSift = (chain, targetValue, nodeTypeNames) => {
  ensureIndexByTypedChain(chain, nodeTypeNames)

  const nodesByKeyValue = getNodesByTypedChain(
    chain,
    targetValue,
    nodeTypeNames
  )

  if (chain.join(`,`) === `id`) {
    if (nodesByKeyValue) {
      // There are cases (and tests) where an id does not exist
      return [nodesByKeyValue]
    }
  } else if (nodesByKeyValue?.size > 0) {
    return [...nodesByKeyValue]
  }

  // If we couldn't find the needle then maybe sift can, for example if the
  // schema contained a proxy; `slug: String @proxy(from: "slugInternal")`
  return undefined
}

/**
 * Filters and sorts a list of nodes using mongodb-like syntax.
 *
 * @param args raw graphql query filter/sort as an object
 * @property args.nodes The nodes array to run sift over (Optional
 *   will load itself if not present)
 * @property {boolean | number | string}args.type gqlType. See build-node-types
 * @property {boolean} args.firstOnly true if you want to return only the first
 *   result found. This will return a collection of size 1. Not a single element
 * @property {{filter?: Object, sort?: Object}} args.queryArgs
 * @returns Collection of results. Collection will be limited to 1
 *   if `firstOnly` is true
 */
const runFilterAndSort = (args: Object) => {
  const {
    queryArgs: { filter, sort } = { filter: {}, sort: {} },
    resolvedFields = {},
    firstOnly = false,
    nodeTypeNames,
  } = args

  let result = applyFilters(
    args,
    filter,
    nodeTypeNames,
    firstOnly,
    resolvedFields
  )

  return sortNodes(result, sort, resolvedFields)
}

exports.runSift = runFilterAndSort

/**
 * Applies filter. First through a simple approach, which is much faster than
 * running sift, but not as versatile and correct. If no nodes were found then
 * it falls back to filtering through sift.
 *
 * @param {Object} args
 * @param {Object | undefined} filter
 * @param {Array<string>} nodeTypeNames
 * @param {boolean} firstOnly
 * @returns {Array<Node> | undefined} Collection of results. Collection will be
 *   limited to 1 if `firstOnly` is true
 */
const applyFilters = (args, filter, nodeTypeNames, firstOnly) => {
  let result = filterWithoutSift(filter, nodeTypeNames, firstOnly)
  if (result) {
    if (firstOnly) {
      return result.slice(0, 1)
    }
    return result
  }

  return filterWithSift(args)
}

/**
 * Check if the filter is "flat" (single leaf) and an "eq". If so, uses custom
 * indexes based on filter and types and returns any result it finds.
 * If conditions are not met or no nodes are found, returns undefined.
 *
 * @param {Object | undefined} filter
 * @param {Array<string>} nodeTypeNames
 * @returns {Array|undefined} Collection of results
 */
const filterWithoutSift = (filter, nodeTypeNames) => {
  if (!filter) {
    return undefined
  }

  // Filter can be any struct of {a: {b: {c: {eq: "x"}}}} and we want to confirm
  // there is exactly one leaf in this structure and that this leaf is `eq`. The
  // actual names are irrelevant, they are a chain of props on a Node.

  let flatChain = getFlatPropertyChain(filter)
  if (!flatChain) {
    return undefined
  }

  // `flatChain` should now be like:
  //   `filter = {this: {is: {the: {chain: {eq: needle}}}}}`
  //  ->
  //   `['this', 'is', 'the', 'chain', 'eq', needle]`
  let targetValue = flatChain.pop()
  let lastPath = flatChain.pop()

  // This can also be `ne`, `in` or any other grapqhl comparison op
  if (lastPath !== `eq`) {
    return undefined
  }

  return runFlatFilterWithoutSift(flatChain, targetValue, nodeTypeNames)
}

/**
 * Use sift to apply filters
 *
 * @param {Object} args
 * @returns {Array<Node> | undefined | null} Collection of results. Collection
 *   will be limited to 1 if `firstOnly` is true
 */
const filterWithSift = args => {
  let nodes = []

  args.nodeTypeNames.forEach(typeName => addResolvedNodes(typeName, nodes))

  return runSiftOnNodes(nodes, args)
}

/**
 * Given a list of filtered nodes and sorting parameters, sort the nodes
 *
 * @param {Array<Node>} nodes Should be all nodes of given type(s)
 * @param args raw graphql query filter/sort as an object
 * @returns {Array<Node> | undefined | null} Collection of results. Collection
 *   will be limited to 1 if `firstOnly` is true
 */
const runSiftOnNodes = (nodes, args) => {
  const {
    queryArgs: { filter } = { filter: {} },
    firstOnly = false,
    resolvedFields = {},
    nodeTypeNames,
  } = args

  let siftFilter = getFilters(
    liftResolvedFields(toDottedFields(prepareQueryArgs(filter)), resolvedFields)
  )

  // If the the query for single node only has a filter for an "id"
  // using "eq" operator, then we'll just grab that ID and return it.
  if (isEqId(siftFilter)) {
    const node = getNode(siftFilter[0].id.$eq)

    if (
      !node ||
      (node.internal && !nodeTypeNames.includes(node.internal.type))
    ) {
      if (firstOnly) {
        return []
      }
      return null
    }

    return [node]
  }

  if (firstOnly) {
    return handleFirst(siftFilter, nodes)
  } else {
    return handleMany(siftFilter, nodes, resolvedFields)
  }
}

exports.runSiftOnNodes = runSiftOnNodes

/**
 * Given a list of filtered nodes and sorting parameters, sort the nodes
 *
 * @param {Array<Node> | undefined | null} nodes Pre-filtered list of nodes
 * @param {Object | undefined} sort Sorting arguments
 * @param resolvedFields
 * @returns {Array<Node> | undefined | null} Same as input, except sorted
 */
const sortNodes = (nodes, sort, resolvedFields) => {
  if (!sort || nodes?.length <= 1) {
    return nodes
  }

  // create functions that return the item to compare on
  const dottedFields = objectToDottedField(resolvedFields)
  const dottedFieldKeys = Object.keys(dottedFields)
  const sortFields = sort.fields
    .map(field => {
      if (
        dottedFields[field] ||
        dottedFieldKeys.some(key => field.startsWith(key))
      ) {
        return `__gatsby_resolved.${field}`
      } else {
        return field
      }
    })
    .map(field => v => getValueAt(v, field))
  const sortOrder = sort.order.map(order => order.toLowerCase())

  return _.orderBy(nodes, sortFields, sortOrder)
}
