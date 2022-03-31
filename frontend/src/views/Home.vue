<template>
  <div class="home">
    <div class="btn-list">
      <el-button
        type="primary"
        v-for="item in btnList"
        :key="item.code"
        @click="togglePreview(item.code)"
      >
        {{ item.name }}
      </el-button>
    </div>
    <!-- 视频统计 -->
    <div class="echart-item" v-if="currentCode == 'video'">
      <echarts
        id="one"
        title="课程近十天增加学习人数"
        :yAxis="videoLearnPerson.yAxis"
        :series="videoLearnPerson.series"
      />
      <echarts
        id="two"
        title="课程近十天增加购买/报名人数"
        :yAxis="videoBuyPerson.yAxis"
        :series="videoBuyPerson.series"
      />
    </div>

    <!-- 机构统计 -->
    <div class="echart-item" v-if="currentCode == 'org'">
      <echarts
        id="three"
        title="机构学习人数最多视频"
        :yAxis="orgMostLearnPerson.yAxis"
        :series="orgMostLearnPerson.series"
      />
      <echarts
        id="four"
        title="机构课程平均所在页数"
        :yAxis="orgVideoPage.yAxis"
        :series="orgVideoPage.series"
      />
      <echarts
        id="five"
        title="机构近十天增加购买/报名人数最多的视频"
        :yAxis="orgCurrentMostByVideo.yAxis"
        :series="orgCurrentMostByVideo.series"
      />
    </div>

    <!-- 视频分类统计 -->
    <div class="echart-item" v-if="currentCode == 'videoType'">
      <echarts
        id="six"
        title="累计免费视频学习人数最多的分类"
        :yAxis="videoTypeTotalLearnPerson.yAxis"
        :series="videoTypeTotalLearnPerson.series"
      />
      <echarts
        id="seven"
        title="近十天免费视频学习人数最多的分类"
        :yAxis="videoTypeCurrentFreeLearnPerson.yAxis"
        :series="videoTypeCurrentFreeLearnPerson.series"
      />
      <echarts
        id="eight"
        title="近十天（免费 + 收费）视频学习人数最多的分类"
        :yAxis="videoTypeCurrentFreeAndChargeLearnPerson.yAxis"
        :series="videoTypeCurrentFreeAndChargeLearnPerson.series"
      />
    </div>
  </div>
</template>

<script>
import Echarts from "@/components/echarts.vue";

import { getVideoData, getOrgData, getVideoTypeData } from "@/request/api.js";
export default {
  name: "Home",
  components: {
    Echarts,
  },
  data() {
    return {
      currentCode: "video",
      btnList: [
        {
          name: "视频数据统计图",
          code: "video",
        },
        {
          name: "机构数据统计图",
          code: "org",
        },
        {
          name: "视频分类数据统计",
          code: "videoType",
        },
      ],

      // 视频统计
      videoLearnPerson: {
        yAxis: [],
        series: [],
      }, // 课程近十天增加学习人数
      videoBuyPerson: {
        yAxis: [],
        series: [],
      }, // 课程近十天增加购买/报名人数

      // 机构数据
      orgMostLearnPerson: {
        yAxis: [],
        series: [],
      }, // 机构学习人数最多视频
      orgVideoPage: {
        yAxis: [],
        series: [],
      }, // 机构课程平均所在页数
      orgCurrentMostByVideo: {
        yAxis: [],
        series: [],
      }, // 机构近十天增加购买/报名人数最多的视频

      // 视频分类
      videoTypeTotalLearnPerson: { yAxis: [], series: [] }, // 累计免费视频学习人数最多的分类前十排行
      videoTypeCurrentFreeLearnPerson: { yAxis: [], series: [] }, // 近十天免费视频学习人数最多的分类前十排行
      videoTypeCurrentFreeAndChargeLearnPerson: { yAxis: [], series: [] }, // 近十天（免费 + 收费）视频学习人数最多的分类前十排行
    };
  },
  created() {
    this.fetchVideoData();
  },
  methods: {
    togglePreview(code) {
      this.currentCode = code;
      if (code == "video") {
        this.fetchVideoData();
      } else if (code == "org") {
        this.fetchOrgData();
      } else {
        this.fetchVideoTypeData();
      }
    },
    fetchVideoData() {
      getVideoData().then((res) => {
        const { add_person_num_10d, add_study_num_10d } = res;
        // 视频学习增加人数数据
        this.videoLearnPerson.yAxis = add_person_num_10d?.map(
          (item) => item.name
        );
        this.videoLearnPerson.series = add_person_num_10d?.map(
          (item) => item.value
        );

        // 课程近十天增加购买/报名人数
        this.videoBuyPerson.yAxis = add_study_num_10d?.map((item) => item.name);
        this.videoBuyPerson.series = add_study_num_10d?.map(
          (item) => item.value
        );
      });
    },
    fetchOrgData() {
      getOrgData().then((res) => {
        const { max_study_video, avg_page, add_person_num_10d_max_video } = res;
        // 机构学习人数最多视频
        this.orgMostLearnPerson.yAxis = max_study_video?.map(
          (item) => item.name
        );
        this.orgMostLearnPerson.series = max_study_video?.map(
          (item) => item.value
        );

        // 机构课程平均所在页数
        this.orgVideoPage.yAxis = avg_page?.map((item) => item.name);
        this.orgVideoPage.series = avg_page?.map((item) => item.value);

        // 机构近十天增加购买/报名人数最多的视频
        this.orgCurrentMostByVideo.yAxis = add_person_num_10d_max_video?.map(
          (item) => item.name
        );
        this.orgCurrentMostByVideo.series = add_person_num_10d_max_video?.map(
          (item) => item.value
        );
      });
    },
    fetchVideoTypeData() {
      getVideoTypeData().then((res) => {
        const {
          max_study_video,
          add_study_num_10d,
          add_person_num_10d_max_video,
        } = res;
        // 累计免费视频学习人数最多的分类前十排行
        this.videoTypeTotalLearnPerson.yAxis = max_study_video?.map(
          (item) => item.name
        );
        this.videoTypeTotalLearnPerson.series = max_study_video?.map(
          (item) => item.value
        );
        // 近十天免费视频学习人数最多的分类前十排行
        this.videoTypeCurrentFreeLearnPerson.yAxis = add_study_num_10d?.map(
          (item) => item.name
        );
        this.videoTypeCurrentFreeLearnPerson.series = add_study_num_10d?.map(
          (item) => item.value
        );

        // 近十天（免费 + 收费）视频学习人数最多的分类前十排行
        this.videoTypeCurrentFreeAndChargeLearnPerson.yAxis =
          add_person_num_10d_max_video?.map((item) => item.name);
        this.videoTypeCurrentFreeAndChargeLearnPerson.series =
          add_person_num_10d_max_video?.map((item) => item.value);
      });
    },
  },
};
</script>

<style lang="scss" scoped>
.home {
  width: 100%;
  padding: 20px;
  box-sizing: border-box;

  .btn-list {
    margin-bottom: 20px;
  }
  .echart-item {
    display: flex;
    flex-wrap: wrap;
  }
}
</style>
