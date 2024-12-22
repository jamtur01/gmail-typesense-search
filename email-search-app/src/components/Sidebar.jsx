// src/components/Sidebar.jsx
import React from 'react';
import { Drawer, IconButton, Typography } from '@mui/material';
import { Panel, RefinementList, SortBy } from 'react-instantsearch-dom';
import MenuIcon from '@mui/icons-material/Menu';
import CloseIcon from '@mui/icons-material/Close';

const Sidebar = () => {
  const [open, setOpen] = React.useState(false);

  const toggleDrawer = (state) => () => {
    setOpen(state);
  };

  return (
    <>
      <IconButton
        color="inherit"
        aria-label="open drawer"
        edge="start"
        onClick={toggleDrawer(true)}
        style={{ position: 'fixed', top: 16, left: 16, zIndex: 1300 }}
      >
        <MenuIcon />
      </IconButton>
      <Drawer
        anchor="left"
        open={open}
        onClose={toggleDrawer(false)}
        PaperProps={{
          style: { width: 240, padding: '16px' },
        }}
      >
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <Typography variant="h6">Filters</Typography>
          <IconButton onClick={toggleDrawer(false)} aria-label="close drawer">
            <CloseIcon />
          </IconButton>
        </div>
        <Panel header="Sender">
          <RefinementList attribute="sender" />
        </Panel>
        <Panel header="Recipients">
          <RefinementList attribute="recipients" />
        </Panel>
        <Panel header="Labels">
          <RefinementList attribute="labels" />
        </Panel>
        <Panel header="Intent">
          <RefinementList attribute="intent" />
        </Panel>
        <Panel header="Sort By">
          <SortBy
            defaultRefinement="emails"
            items={[
              { label: 'Date Descending', value: 'emails' },
              { label: 'Date Ascending', value: 'emails/sort/date:asc' },
            ]}
          />
        </Panel>
      </Drawer>
    </>
  );
};

export default Sidebar;

