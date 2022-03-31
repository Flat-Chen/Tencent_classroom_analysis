import axios from "axios";

axios.defaults.timeout = 60000;
axios.defaults.withCredentials = true;

const BASE_URL = "http://192.168.1.15:2005";

axios.defaults.baseURL =
  process.env.NODE_ENV === "development" ? "/" : BASE_URL;

axios.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem("curToken");
    if (!config.headers.hasOwnProperty.call(config.headers, "token") && token) {
      config.headers.token = token;
    }
    return config;
  },
  (error) => {
    return Promise.reject(error);
  }
);

axios.interceptors.response.use(
  (res) => {
    // cancelPending(res.config);
    if (res.data.code == 1001) {
      return res;
    }
    if (!res.data.success) {
      return Promise.resolve(res);
    }
    return res;
  },
  (error) => {
    return Promise.reject(error);
  }
);

// export function post(url, params) {
//   return new Promise((resolve, reject) => {
//     axios
//       .post(url, qs.stringify(params))
//       .then(
//         (response) => {
//           resolve(response.data);
//         },
//         (err) => {
//           reject(err);
//         }
//       )
//       .catch((error) => {
//         reject(error);
//       });
//   });
// }

export function get(url, param) {
  return new Promise((resolve, reject) => {
    axios
      .get(url, {
        params: param,
      })
      .then(
        (response) => {
          resolve(response.data);
        },
        (err) => {
          reject(err);
        }
      )
      .catch((error) => {
        reject(error);
      });
  });
}

export default {
  // post,
  get,
};
