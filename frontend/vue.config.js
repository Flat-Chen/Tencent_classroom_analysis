const path = require("path");
function resolve(dir) {
  return path.join(__dirname, dir);
}
const defaultSettings = require("./src/settings.js");
const name = defaultSettings.title || "Admin"; // page title
module.exports = {
  publicPath: "./",
  outputDir: "dist",
  assetsDir: "./assets",
  lintOnSave: false,
  productionSourceMap: false,
  devServer: {
    open: true,
    host: "0.0.0.0",
    port: 9080,
    disableHostCheck: true,
    proxy: {
      "/api": {
        target: "http://node01:8901/api/",
        changeOrigin: true,
        pathRewrite: {
          "^/api": "",
        },
      },
    },
  },
  configureWebpack: {
    name: name,
    resolve: {
      alias: {
        "@": resolve("src"),
        assets: resolve("src/assets"),
      },
    },
  },
};
