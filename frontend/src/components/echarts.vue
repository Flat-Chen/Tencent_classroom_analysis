<template>
  <div class="bar" :id="id"></div>
</template>

<script>
export default {
  props: {
    id: {
      type: String,
    },
    title: {
      type: String,
    },
    yAxis: {
      type: Array,
    },
    series: {
      type: Array,
    },
  },
  data() {
    return {
      myCharts: null,
    };
  },
  computed: {
    getOptions() {
      const option = {
        title: {
          text: this.title,
        },
        tooltip: {
          trigger: "axis",
          axisPointer: {
            type: "shadow",
          },
        },
        legend: {
          show: false,
        },
        grid: {
          left: "3%",
          right: "4%",
          bottom: "3%",
          containLabel: true,
        },
        xAxis: {
          type: "value",
          boundaryGap: [0, 0.01],
        },
        yAxis: {
          type: "category",
          data: this.yAxis,
        },
        series: {
          name: "数量",
          type: "bar",
          data: this.series,
        },
      };
      return option;
    },
  },
  watch: {
    series: {
      deep: true,
      handler: function () {
        this.updateEcharts();
      },
    },
  },
  mounted() {
    this.$nextTick(() => {
      this.echartsInit();
    });
  },
  methods: {
    echartsInit() {
      this.myCharts = this.$echarts.init(document.getElementById(this.id));
      console.log(this.getOptions);
      this.myCharts.setOption(this.getOptions);
    },
    updateEcharts() {
      if (!this.myCharts) return;
      this.myCharts.setOption(this.getOptions);
    },
  },
};
</script>
<style lang="scss" scoped>
.bar {
  width: 48%;
  height: 500px;
  margin-right: 2%;
}
</style>
