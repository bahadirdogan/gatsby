/* @flow */

const { store } = require(`./index`)

/**
 * Get all nodes from redux store.
 *
 * @returns {Array}
 */
const getNodes = () => {
  const nodes = store.getState().nodes
  if (nodes) {
    return Array.from(nodes.values())
  } else {
    return []
  }
}

exports.getNodes = getNodes

/** Get node by id from store.
 *
 * @param {string} id
 * @returns {Object}
 */
const getNode = id => store.getState().nodes.get(id)

exports.getNode = getNode

/**
 * Get all nodes of type from redux store.
 *
 * @param {string} type
 * @returns {Array}
 */
const getNodesByType = type => {
  const nodes = store.getState().nodesByType.get(type)
  if (nodes) {
    return Array.from(nodes.values())
  } else {
    return []
  }
}

exports.getNodesByType = getNodesByType

/**
 * Get all type names from redux store.
 *
 * @returns {Array}
 */
const getTypes = () => Array.from(store.getState().nodesByType.keys())

exports.getTypes = getTypes

/**
 * Determine if node has changed.
 *
 * @param {string} id
 * @param {string} digest
 * @returns {boolean}
 */
exports.hasNodeChanged = (id, digest) => {
  const node = store.getState().nodes.get(id)
  if (!node) {
    return true
  } else {
    return node.internal.contentDigest !== digest
  }
}

/**
 * Get node and save path dependency.
 *
 * @param {string} id
 * @param {string} path
 * @returns {Object} node
 */
exports.getNodeAndSavePathDependency = (id, path) => {
  const createPageDependency = require(`./actions/add-page-dependency`)
  const node = getNode(id)
  createPageDependency({ path, nodeId: id })
  return node
}

exports.saveResolvedNodes = async (nodeTypeNames, resolver) => {
  for (const typeName of nodeTypeNames) {
    const nodes = store.getState().nodesByType.get(typeName)
    const resolvedNodes = new Map()
    if (nodes) {
      for (const node of nodes.values()) {
        const resolved = await resolver(node)
        resolvedNodes.set(node.id, resolved)
      }
      store.dispatch({
        type: `SET_RESOLVED_NODES`,
        payload: {
          key: typeName,
          nodes: resolvedNodes,
        },
      })
    }
  }
}

const getResolvedNodesCache = () => store.getState().resolvedNodesCache

exports.getResolvedNodesCache = getResolvedNodesCache

/**
 * Get node and save path dependency.
 *
 * @param {string} typeName
 * @param {string} id
 * @returns {Object|void} node
 */
const getResolvedNode = (typeName, id) => {
  const { nodesByType, resolvedNodesCache } = store.getState()
  const nodes /*: Map<mixed> */ = nodesByType.get(typeName)

  if (!nodes) {
    return null
  }

  let node = nodes.get(id)

  if (!node) {
    return null
  }

  const resolvedNodes = resolvedNodesCache.get(typeName)

  if (resolvedNodes) {
    node.__gatsby_resolved = resolvedNodes.get(id)
  }

  return node
}

exports.getResolvedNode = getResolvedNode

const addResolvedNodes = (typeName, arr) => {
  const { nodesByType, resolvedNodesCache } = store.getState()
  const nodes /*: Map<mixed> */ = nodesByType.get(typeName)

  if (!nodes) {
    return
  }

  const resolvedNodes = resolvedNodesCache.get(typeName)

  nodes.forEach(node => {
    if (resolvedNodes) {
      node.__gatsby_resolved = resolvedNodes.get(node.id)
    }
    arr.push(node)
  })
}

exports.addResolvedNodes = addResolvedNodes

// TODO: This local cache is "global" to the nodejs instance. It should be
//       reset after each point where nodes are mutated. This is currently
//       not the case. I don't think we want to persist this cache in redux.
//       (This works currently fine for a `build`, but won't for `develop`)
let mappedByKey

const ensureIndexByTypedChain = (chain, nodeTypeNames) => {
  const chained = chain.join(`+`)

  if (!mappedByKey) {
    mappedByKey = new Map()
  }

  const nodeTypeNamePrefix = nodeTypeNames.join(`,`) + `/`
  // The format of the typedKey is `type,type/path+to+eqobj`
  const typedKey = nodeTypeNamePrefix + chained

  let byKeyValue = mappedByKey.get(typedKey)
  if (byKeyValue) {
    return
  }

  const { nodes, resolvedNodesCache } = store.getState()

  byKeyValue = new Map() // Map<node.value, Set<all nodes with this value for this key>>
  mappedByKey.set(typedKey, byKeyValue)

  // TODO: Is it faster to loop through the maps by type instead of all nodes?
  // TODO: Are filters generally interested in internal artifact nodes that are is not related to a page?
  nodes.forEach(node => {
    if (!nodeTypeNames.includes(node.internal.type)) {
      return
    }

    // There can be a filter that targets `__gatsby_resolved`
    if (!node.__gatsby_resolved) {
      const typeName = node.internal.type
      const resolvedNodes = resolvedNodesCache.get(typeName)
      node.__gatsby_resolved = resolvedNodes?.get(node.id)
    }

    let v = node
    let i = 0
    while (i < chain.length && v) {
      const nextProp = chain[i++]
      v = v[nextProp]
    }

    if (
      (typeof v !== `string` &&
        typeof v !== `number` &&
        typeof v !== `boolean`) ||
      i !== chain.length
    ) {
      // Not sure whether this is supposed to happen, but this means that either
      // - The node chain ended with `undefined`, or
      // - The node chain ended in something other than a primitive, or
      // - A part in the chain in the object was not an object
      return
    }

    // Special case `id` as that bucket never receives more than one element
    if (chained === `id`) {
      // Note: this is not a duplicate from `nodes` because this set only
      //       contains nodes of this type. Page nodes are a subset of all nodes
      byKeyValue.set(v, node)
    } else {
      let set = byKeyValue.get(v)
      if (!set) {
        set = new Set()
        byKeyValue.set(v, set)
      }
      set.add(node)
    }
  })
}

exports.ensureIndexByTypedChain = ensureIndexByTypedChain

const getNodesByTypedChain = (chain, value, nodeTypeNames) => {
  const key = chain.join(`+`)

  const typedKey = nodeTypeNames.join(`,`) + `/` + key

  let byTypedKey = mappedByKey?.get(typedKey)
  return byTypedKey?.get(value)
}

exports.getNodesByTypedChain = getNodesByTypedChain
