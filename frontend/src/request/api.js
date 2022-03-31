import http from "./http";

export const getVideoData = (data) => {
  return http.get("api/rank/video", data);
};

export const getOrgData = (data) => {
  return http.get("api/rank/organ", data);
};

export const getVideoTypeData = (data) => {
  return http.get("api/rank/video_type", data);
};
