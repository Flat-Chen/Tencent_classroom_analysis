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
      console.log(this.series);
      const option = {
        title: {
          text: this.title,
          left: "center",
        },
        tooltip: {
          trigger: "item",
        },
        legend: {
          orient: "horizontal",
          bottom: 0,
        },
        series: [
          {
            name: this.title,
            type: "pie",
            radius: "50%",
            data: this.series,
            emphasis: {
              itemStyle: {
                shadowBlur: 10,
                shadowOffsetX: 0,
                shadowColor: "rgba(0, 0, 0, 0.5)",
              },
            },
          },
        ],
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
