// This interceptor will be passed every request and response. We will take that request and response
// and post a message to the window. This will allow us to access this message in the content script. This
// is all to make the manifest v3 happy.

/**
 * Creates an interceptor with optional custom toJson function
 * @param {Function} [customToJson] - Optional custom function to serialize messages to JSON.
 *                                     Should accept a message object and return a JSON-serializable object.
 * @returns {Function} The interceptor function
 */
const createInterceptor = (customToJson) => {
  // Default to using the message's toJson method if no custom function is provided
  const toJson = customToJson || ((message) => message.toJson());

  const post = (data) =>
    window.postMessage({ type: "__GRPCWEB_DEVTOOLS__", ...data }, "*");

  // A serialization failure must never break the application's RPC.
  const serialize = (message) => {
    try {
      return toJson(message);
    } catch (e) {
      return { __devtoolsSerializationError: String((e && e.message) || e) };
    }
  };

  const toError = (e) => ({ message: e && e.message, code: e && e.code });

  const isAsyncIterable = (value) =>
    value != null && typeof value[Symbol.asyncIterator] === "function";

  const unary = async (next, req) => {
    const method = req.method.name;
    const methodType = "unary";
    const request = serialize(req.message);
    try {
      const resp = await next(req);
      post({ methodType, method, request, response: serialize(resp.message) });
      return resp;
    } catch (e) {
      post({ methodType, method, request, error: toError(e) });
      throw e;
    }
  };

  // Streaming calls are logged the same way inject.js logs grpc-web streams:
  // one entry per request message, one per response message, then "EOF" or an error.
  const streaming = async (next, req) => {
    const method = req.method.name;
    const methodType = "server_streaming";

    let outgoing = req;
    if (isAsyncIterable(req.message)) {
      // connect-es >= 0.9: request messages are an AsyncIterable, even for server streaming.
      const requests = req.message;
      outgoing = {
        ...req,
        message: (async function* () {
          for await (const message of requests) {
            post({ methodType, method, request: serialize(message) });
            yield message;
          }
        })(),
      };
    } else if (req.message !== undefined) {
      post({ methodType, method, request: serialize(req.message) });
    }

    let resp;
    try {
      resp = await next(outgoing);
    } catch (e) {
      post({ methodType, method, error: toError(e) });
      throw e;
    }

    if (isAsyncIterable(resp.message)) {
      const responses = resp.message;
      return {
        ...resp,
        message: (async function* () {
          try {
            for await (const message of responses) {
              post({ methodType, method, response: serialize(message) });
              yield message;
            }
          } catch (e) {
            post({ methodType, method, error: toError(e) });
            throw e;
          }
          post({ methodType, method, response: "EOF" });
        })(),
      };
    }

    if (typeof resp.read === "function") {
      // Legacy connect-web (< 0.9) stream responses expose read() instead of an AsyncIterable.
      return {
        ...resp,
        async read() {
          let result;
          try {
            result = await resp.read();
          } catch (e) {
            post({ methodType, method, error: toError(e) });
            throw e;
          }
          if (result.value) {
            post({ methodType, method, response: serialize(result.value) });
          } else if (result.done) {
            post({ methodType, method, response: "EOF" });
          }
          return result;
        },
      };
    }

    return resp;
  };

  return (next) => (req) =>
    req.stream ? streaming(next, req) : unary(next, req);
};

// Default interceptor for backward compatibility
window.__CONNECT_WEB_DEVTOOLS__ = createInterceptor();

// Export the factory function for custom usage
window.__CREATE_CONNECT_WEB_DEVTOOLS__ = createInterceptor;

// Since we are loading inject.js as a script, the order at which it is loaded is not guaranteed.
// So we will publish a custom event that can be used, to be used to assign the interceptor.
const readyEvent = new CustomEvent("connect-web-dev-tools-ready");
window.dispatchEvent(readyEvent);
